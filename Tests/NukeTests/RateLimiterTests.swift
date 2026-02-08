// The MIT License (MIT)
//
// Copyright (c) 2015-2026 Alexander Grebenyuk (github.com/kean).

import Testing
@testable import Nuke

@Suite(.timeLimit(.minutes(5))) @ImagePipelineActor
struct RateLimiterTests {
    // max 2 req/second, so the first 2 requests should be executed immediate, but more than 2 should be queued up
    let rateLimiter = RateLimiter(interval: 1, maxRequestCount: 2)
    @Test func burstIsExecutedImmediately() {
        var isExecuted = Array(repeating: false, count: 4)
        for i in isExecuted.indices {
            rateLimiter.execute {
                isExecuted[i] = true
                return true
            }
        }
        #expect(isExecuted == [true, true, false, false], "Expect first 2 items to be executed immediately")
    }

    @Test func posponedItemsDoNotExtractFromBucket() {
        var isExecuted = Array(repeating: false, count: 4)
        for i in isExecuted.indices {
            rateLimiter.execute {
                isExecuted[i] = true
                return i != 1 // important!
            }
        }
        #expect(isExecuted == [true, true, false, false], "Expect first 2 items to be executed immediately")
    }

    @Test func overflow() async {
        let count = 3
        await confirmation(expectedCount: count) { done in
            for _ in 0..<count {
                await withUnsafeContinuation { continuation in
                    rateLimiter.execute {
                        done()
                        continuation.resume(returning: ())
                        return true
                    }
                }
            }
        }
    }

    // MARK: - Edge Cases

    @Test func burstOfOneExecutesSingleItemImmediately() {
        // GIVEN - rate limiter that only allows 1 immediate execution
        let limiter = RateLimiter(interval: 10, maxRequestCount: 1)
        var executed = [false, false]

        // WHEN
        limiter.execute { executed[0] = true; return true }
        limiter.execute { executed[1] = true; return true }

        // THEN - only the first item runs immediately; the second is deferred
        #expect(executed[0] == true)
        #expect(executed[1] == false)
    }
}
