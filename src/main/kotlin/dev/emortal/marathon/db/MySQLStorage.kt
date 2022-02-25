package dev.emortal.marathon.db

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import dev.emortal.marathon.MarathonExtension
import dev.emortal.marathon.TimeFrame
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import java.io.InputStream
import java.net.URLEncoder
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.sql.PreparedStatement
import java.sql.Types
import java.util.*


class MySQLStorage : Storage() {

    init {
        prepareStatement(
            "CREATE TABLE IF NOT EXISTS marathon (`player` BINARY(16), `highscore` INT, `time` BIGINT, `time_frame` VARCHAR(127))",
            PreparedStatement::executeUpdate
        )
    }

    fun drop() {
        prepareStatement("DROP TABLE marathon", PreparedStatement::executeUpdate)
    }

    override fun setHighscore(player: UUID, highscore: Highscore, timeFrame: TimeFrame?) {
        runBlocking {
            launch {
                prepareStatement("DELETE FROM marathon WHERE player=? AND time_frame=?") { statement ->
                    statement.setBinaryStream(1, player.toInputStream())
                    statement.setTimeFrame(2, timeFrame)

                    statement.executeUpdate()
                }

                prepareStatement("INSERT INTO marathon VALUES(?, ?, ?, ?)") { statement ->
                    statement.setBinaryStream(1, player.toInputStream())
                    statement.setInt(2, highscore.score)
                    statement.setLong(3, highscore.time)
                    statement.setTimeFrame(4, timeFrame)

                    statement.executeUpdate()
                }
            }
        }
    }

    override suspend fun getHighscoreAsync(player: UUID, timeFrame: TimeFrame?): Highscore? = coroutineScope {
        return@coroutineScope async {
            val results = prepareStatement("SELECT highscore, time FROM marathon WHERE player=? AND time_frame=?") { statement ->
                statement.setBinaryStream(1, player.toInputStream())
                statement.setTimeFrame(2, timeFrame)

                statement.executeQuery()
            }

            results.use {
                var highscore: Int? = null
                var time: Long? = null
                if (results.next()) {
                    highscore = results.getInt(1)
                    time = results.getLong(2)
                }

                if (highscore == null || time == null) return@async null

                return@async Highscore(highscore, time)
            }
        }.await()
    }

    override suspend fun getTopHighscoresAsync(highscoreCount: Int, timeFrame: TimeFrame?): Map<UUID, Highscore> = coroutineScope {
        return@coroutineScope async {
            val results = prepareStatement("SELECT * FROM marathon ORDER BY highscore DESC, time ASC WHERE time_frame=? LIMIT $highscoreCount") {
                it.setTimeFrame(1, timeFrame)
                return@prepareStatement it.executeQuery()
            }

            results.use {
                val map = mutableMapOf<UUID, Highscore>()
                while (it.next()) {
                    val uuid = it.getBinaryStream("player").toUUID()
                    val score = it.getInt("highscore")
                    val time = it.getLong("time")

                    map[uuid] = Highscore(score, time)
                }

                return@async map
            }
        }.await()
    }

    override suspend fun getPlacementAsync(score: Int, timeFrame: TimeFrame?): Int? = coroutineScope {
        return@coroutineScope async {
            return@async prepareStatement("SELECT COUNT(DISTINCT player) + 1 AS total FROM marathon WHERE highscore > ? AND time_frame = ?") {
                it.setInt(1, score)
                it.setTimeFrame(2, timeFrame)
                it.executeQuery().use { results ->
                    if (results.next()) {
                        return@prepareStatement results.getInt("total")
                    }
                    return@prepareStatement null
                }
            }
        }.await()
    }

    override fun createHikari(): HikariDataSource {
        val dbConfig = MarathonExtension.databaseConfig

        val dbName = URLEncoder.encode(dbConfig.tableName, StandardCharsets.UTF_8.toString())
        val dbUsername = URLEncoder.encode(dbConfig.username, StandardCharsets.UTF_8.toString())
        val dbPassword = URLEncoder.encode(dbConfig.password, StandardCharsets.UTF_8.toString())

        //172.17.0.1

        val hikariConfig = HikariConfig()
        hikariConfig.jdbcUrl = "jdbc:mysql://${dbConfig.address}:${dbConfig.port}/${dbName}?user=${dbUsername}&password=${dbPassword}"
        hikariConfig.driverClassName = "com.mysql.cj.jdbc.Driver"

        val hikariSource = HikariDataSource(hikariConfig)
        return hikariSource
    }

    private fun PreparedStatement.setTimeFrame(parameterIndex: Int, timeFrame: TimeFrame?) {
        if (timeFrame == null) {
            setNull(parameterIndex, Types.VARCHAR)
        } else {
            setString(parameterIndex, timeFrame.name)
        }
    }

    fun UUID.toInputStream(): InputStream {
        val bb = ByteBuffer.wrap(ByteArray(16))
        bb.putLong(mostSignificantBits)
        bb.putLong(leastSignificantBits)
        return bb.array().inputStream()
    }

    fun InputStream.toUUID(): UUID {
        val byteBuffer = ByteBuffer.wrap(this.readAllBytes())
        val high = byteBuffer.long
        val low = byteBuffer.long
        return UUID(high, low)
    }
}