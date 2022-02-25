package dev.emortal.marathon.db

import com.zaxxer.hikari.HikariDataSource
import dev.emortal.marathon.TimeFrame
import org.intellij.lang.annotations.Language
import java.sql.Connection
import java.sql.PreparedStatement
import java.util.*

abstract class Storage {

    abstract fun setHighscore(player: UUID, highscore: Highscore, timeFrame: TimeFrame? = null)
    abstract suspend fun getHighscoreAsync(player: UUID, timeFrame: TimeFrame? = null): Highscore?
    abstract suspend fun getTopHighscoresAsync(highscoreCount: Int = 10, timeFrame: TimeFrame? = null): Map<UUID, Highscore>?
    abstract suspend fun getPlacementAsync(score: Int, timeFrame: TimeFrame? = null): Int?

    val hikari = createHikari()
    abstract fun createHikari(): HikariDataSource

    fun getConnection(): Connection = hikari.connection

    fun <T> prepareStatement(@Language("SQL") sql: String, block: (PreparedStatement) -> T): T {
        return getConnection().use { conn ->
            conn.prepareStatement(sql).use(block)
        }
    }

}