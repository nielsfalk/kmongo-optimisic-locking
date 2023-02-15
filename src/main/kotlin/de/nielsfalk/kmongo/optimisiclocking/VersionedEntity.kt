package de.nielsfalk.kmongo.optimisiclocking

interface VersionedEntity<T> {
    val version: Long
}
