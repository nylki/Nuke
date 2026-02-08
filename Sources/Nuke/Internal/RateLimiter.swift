// The MIT License (MIT)
//
// Copyright (c) 2015-2026 Alexander Grebenyuk (github.com/kean).

import Foundation

/// Controls the rate at which the work is executed. Uses the classic [token
/// bucket](https://en.wikipedia.org/wiki/Token_bucket) algorithm.
///
/// The main use case for rate limiter is to support large (infinite) collections
/// of images by preventing thrashing of underlying systems, primarily URLSession.
///
/// The implementation supports quick bursts of requests which can be executed
/// without any delays when "the bucket is full". This is important to prevent
/// rate limiter from affecting "normal" requests flow.
@ImagePipelineActor
final class RateLimiter {
    private var bucket: TokenBucket
    private var pending = LinkedList<Work>() // fast append, fast remove first
    private var isExecutingPendingTasks = false
    

    typealias Work = () -> Bool

    /// Initializes the `RateLimiter` with the given configuration.
    /// - parameters:
    ///   - interval: The time interval to rate limit (now - interval) as a sliding window
    ///   - maxRequestCount: Maximum number of requests which can be executed during the interval.
    init(interval: Double, maxRequestCount: Double) {
        self.bucket = TokenBucket(interval: interval, maxRequestCount: maxRequestCount)
    }

    /// - parameter closure: Returns `true` if the closure was executed, `false`
    /// if the work was cancelled.
    func execute( _ work: @escaping Work) {
        if !pending.isEmpty || !bucket.execute(work) {
            pending.append(work)
            setNeedsExecutePendingTasks()
        }
    }

    private func setNeedsExecutePendingTasks() {
        guard !isExecutingPendingTasks else {
            return
        }
        isExecutingPendingTasks = true
        // Compute a delay such that by the time the closure is executed the
        // bucket is refilled to a point that is able to execute at least one
        // pending task. With a rate of 80 tasks we expect a refill every ~26 ms
        // or as soon as the new tasks are added.
        let bucketRate = 1000.0 / bucket.rate
        let delay = Int(2.1 * bucketRate) // 14 ms for rate 80 (default)
        let bounds = min(100, max(15, delay))
        Task { @ImagePipelineActor in
            try? await Task.sleep(nanoseconds: UInt64(bounds) * 1_000_000)
            self.executePendingTasks()
        }
    }

    private func executePendingTasks() {
        while let node = pending.first, bucket.execute(node.value) {
            pending.remove(node)
        }
        isExecutingPendingTasks = false
        if !pending.isEmpty { // Not all pending items were executed
            setNeedsExecutePendingTasks()
        }
    }
}

private struct TokenBucket {
    let rate: Double
    private var bucket: Double
    private var lastRefillTimestamp: TimeInterval // last refill timestamp
    private let interval: TimeInterval
    private let maxRequestCount: Double // aka burst
    private var intervalTimestamps = LinkedList<TimeInterval>()
    
    /// - parameter rate: Rate (tokens/second) at which bucket is refilled.
    /// - parameter burst: Bucket size (maximum number of tokens).
    init(interval: Double, maxRequestCount: Double) {
        self.interval = interval
        self.maxRequestCount = maxRequestCount
        self.rate = maxRequestCount / interval
        self.bucket = maxRequestCount
        self.lastRefillTimestamp = CFAbsoluteTimeGetCurrent()
    }

    /// Returns `true` if the closure was executed, `false` if dropped.
    mutating func execute(_ work: () -> Bool) -> Bool {
        refill()
        guard bucket >= 1.0 else {
            return false // bucket is empty
        }
        
        guard Double(requestCount(in: interval)) < maxRequestCount else {
            return false // exceeding max requests in sliding time interval
        }
        intervalTimestamps.append(CFAbsoluteTimeGetCurrent())
        if work() {
            bucket -= 1.0
        }
        // If work was cancelled (returned false), don't reduce the bucket
        return true
    }
    
    private func pruneSlidingWindow(now: TimeInterval, maxAge: TimeInterval = 10) {
        while let first = intervalTimestamps.first, (now - first.value) >= maxAge {
            intervalTimestamps.remove(first)
        }
    }
    
    private func requestCount(in timeInterval: TimeInterval) -> Int {
        pruneSlidingWindow(now: CFAbsoluteTimeGetCurrent(), maxAge: timeInterval)
        return intervalTimestamps.count
    }

    private mutating func refill() {
        let now = CFAbsoluteTimeGetCurrent()
        bucket += rate * max(0, now - lastRefillTimestamp) // rate * (time delta)
        lastRefillTimestamp = now
        if bucket > maxRequestCount { // prevent bucket overflow
            bucket = maxRequestCount
        }
    }
}
