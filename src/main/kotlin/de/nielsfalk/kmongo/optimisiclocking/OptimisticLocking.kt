package de.nielsfalk.kmongo.optimisiclocking

import com.mongodb.client.model.ReplaceOptions
import com.mongodb.client.result.UpdateResult
import com.mongodb.reactivestreams.client.MongoCollection
import kotlinx.coroutines.reactive.awaitFirstOrNull
import kotlinx.coroutines.reactive.awaitSingle
import kotlinx.coroutines.reactive.publish
import org.bson.BsonDocument
import org.bson.BsonInt64
import org.bson.conversions.Bson
import org.litote.kmongo.coroutine.CoroutineCollection
import org.reactivestreams.Publisher


val <T : VersionedEntity<*>> CoroutineCollection<T>.optimisticLocking: CoroutineCollection<T>
    get() = CoroutineCollection(collection.optimisticLocking)
private val <TDocument> MongoCollection<TDocument>.optimisticLocking: MongoCollection<TDocument>
    get() {
        val original = this
        return object : MongoCollection<TDocument> by original {

            override fun <NewTDocument : Any?> withDocumentClass(clazz: Class<NewTDocument>): MongoCollection<NewTDocument> =
                original.withDocumentClass(clazz).optimisticLocking

            override fun replaceOne(filter: Bson, replacement: TDocument, options: ReplaceOptions): Publisher<UpdateResult?> =
                publish {
                    val result = if (replacement.version == null)
                        original.replaceOne(filter, replacement, options)
                    else {
                        val found = find(filter).awaitFirstOrNull()
                        if (replacement.version == 0L) {
                            if (!options.isUpsert)
                                throw IllegalStateException("upsert need to be enabled with verison 0")
                            if (found == null)
                                original.replaceOne(filter, replacement.incrementVersion, options)
                            else throw OptimisticLockingException("can not insert $replacement on $filter found was $found")
                        } else {
                            if (replacement.version == found?.version)
                                original.replaceOne(filter, replacement.incrementVersion, options.upsert(false))
                            else throw OptimisticLockingException("can not update $replacement on $filter current version is ${found?.version}")
                        }
                    }
                    send(result.awaitSingle())
                }
        }
    }

private val <TDocument> TDocument.incrementVersion: TDocument
    get() {
        val bsonDocument = this as? BsonDocument
        bsonDocument?.version?.let {
            bsonDocument["version"] = BsonInt64(it.inc())
        }
        return this
    }

private val <TDocument> TDocument.version: Long?
    get() {
        val bsonDocument = this as? BsonDocument
        return bsonDocument?.version
    }

private val BsonDocument.version: Long?
    get() = get("version")
        ?.let { it as? BsonInt64 }
        ?.longValue()

class OptimisticLockingException(message: String) : Exception(message)
