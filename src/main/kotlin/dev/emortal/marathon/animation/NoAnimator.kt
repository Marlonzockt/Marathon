package dev.emortal.marathon.animation

import dev.emortal.immortal.game.Game
import net.minestom.server.coordinate.Point
import net.minestom.server.instance.block.Block
import net.minestom.server.timer.Task

class NoAnimator(game: Game) : BlockAnimator(game) {
    override val tasks = mutableListOf<Task>()
    override fun setBlockAnimated(point: Point, block: Block, lastPoint: Point) {
        game.instance.setBlock(point, block)
    }
}