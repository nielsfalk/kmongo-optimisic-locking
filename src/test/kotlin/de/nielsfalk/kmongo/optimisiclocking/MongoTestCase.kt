package de.nielsfalk.kmongo.optimisiclocking

import de.nielsfalk.givenwhenthen.AutoCloseBlock
import de.nielsfalk.givenwhenthen.GivenWhenThenTest
import de.nielsfalk.givenwhenthen.TestExecutable
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.testcontainers.containers.MongoDBContainer
import org.testcontainers.containers.output.Slf4jLogConsumer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers

@Testcontainers
abstract class MongoTestCase(
    vararg scenario: List<TestExecutable<*>>,
    scenarios: MutableList<List<TestExecutable<*>>> = mutableListOf(*scenario),
    beforeEach: (AutoCloseBlock.() -> Unit)? = null,
    afterEach: (() -> Unit)? = null,
    beforeAll: (AutoCloseBlock.() -> Unit)? = null,
    afterAll: (() -> Unit)? = null
) : GivenWhenThenTest(
    scenario = *scenario,
    scenarios = scenarios,
    beforeEach = beforeEach,
    afterEach = afterEach,
    beforeAll = beforeAll,
    afterAll = afterAll
) {
    companion object {
        private val logger: Logger = LoggerFactory.getLogger(MongoTestCase::class.java)

        @Container
        @JvmStatic
        val mongoContainer = MongoDBContainer("mongo:4.4.9")
            .withLogConsumer(
                Slf4jLogConsumer(logger).withSeparateOutputStreams()
            )
    }
}
