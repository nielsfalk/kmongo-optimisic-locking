package de.nielsfalk.kmongo.optimisiclocking

import de.nielsfalk.givenwhenthen.*
import kotlinx.serialization.Contextual
import kotlinx.serialization.Serializable
import org.bson.codecs.pojo.annotations.BsonId
import org.litote.kmongo.Id
import org.litote.kmongo.coroutine.coroutine
import org.litote.kmongo.newId
import org.litote.kmongo.reactivestreams.KMongo
import strikt.api.expectCatching
import strikt.api.expectThat
import strikt.assertions.*


private val luke by lazy { JediWithVersion(name = "Luke", age = 20) }

private val collection
    get() = KMongo.createClient(MongoTestCase.mongoContainer.connectionString)
        .coroutine
        .getDatabase("test")
        .getCollection<JediWithVersion>()
        .optimisticLocking

class OptimisticLockingKtTest : MongoTestCase(
    scenario(
        description { "insert optimistic with version 99" },
        given {
            luke.copy(version = 99)
        },
        `when` {
            expectCatching { collection.save(it) }
        },
        then {
            it.isFailure()
                .isA<OptimisticLockingException>()
                .get { message }.isEqualTo("""can not update {"name": "Luke", "age": 20, "version": ${given.version}} on Filter{fieldName='_id', value=${luke._id}} current version is null""")
        }
    ),
    scenario(
        description { "insert optimistic" },
        `when` {
            collection.save(luke)
        },
        then {
            expectThat(collection.findOneById(luke._id))
                .isNotNull()
                .get { version }.isEqualTo(1)
        }
    ),
    scenario(
        description { "insert again" },
        `when` {
            expectCatching { collection.save(luke) }
        },
        then {
            it.isFailure()
                .get { message }.isEqualTo("""can not insert {"name": "Luke", "age": 20, "version": 0} on Filter{fieldName='_id', value=${luke._id}} found was {"_id": {"${'$'}oid": "${luke._id}"}, "name": "Luke", "age": 20, "version": 1}""")
        }
    ),
    scenario(
        description { "save optimistic" },
        given {
            checkNotNull(collection.findOneById(luke._id))
        },
        `when` {
            collection.save(it)
        },
        then {
            expectThat(collection.findOneById(luke._id))
                .isNotNull()
                .get { version }.isEqualTo(given.version + 1)
        }
    ),
    scenario(
        description { "save with wrong version" },
        `when` {
            expectCatching { collection.save(luke.copy(version = 999)) }
        },
        then {
            it.isFailure()
                .isA<OptimisticLockingException>()
                .get { message }.isEqualTo("""can not update {"name": "Luke", "age": 20, "version": 999} on Filter{fieldName='_id', value=${luke._id}} current version is 2""")
        }
    )
)


interface Entity<T> {
    @Suppress("PropertyName")
    val _id: Id<T>
}

@Serializable
data class JediWithVersion(
    @Contextual
    @BsonId
    override val _id: Id<JediWithVersion> = newId(),
    val name: String,
    val age: Int,
    override val version: Long = 0
) : Entity<JediWithVersion>, VersionedEntity<JediWithVersion>
