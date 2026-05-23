package io.ktredis.cluster

import io.ktredis.protocol.RespValue
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class ClusterTest {

    @Test fun `CRC16-XMODEM check value`() {
        // standard check value of CRC-16/XMODEM for "123456789"
        assertEquals(0x31C3, Crc16.crc16("123456789".toByteArray()))
    }

    @Test fun `slot nam trong 0 den 16383`() {
        for (k in listOf("foo", "bar", "user:1", "abc{def}", "x")) {
            val slot = Crc16.keyHashSlot(k)
            assertTrue(slot in 0 until 16384, "slot $slot out of range")
        }
    }

    @Test fun `hash tag chi hash phan trong ngoac`() {
        // only the part inside {} is hashed -> two keys with the same tag map to the same slot
        assertEquals(
            Crc16.keyHashSlot("{user1000}.following"),
            Crc16.keyHashSlot("{user1000}.followers")
        )
        // key with tag = hash of the tag content only
        assertEquals(Crc16.crc16("abc".toByteArray()) % 16384, Crc16.keyHashSlot("{abc}.whatever"))
        // empty tag {} -> hash the ENTIRE key (not an empty string)
        assertEquals(Crc16.crc16("{}.x".toByteArray()) % 16384, Crc16.keyHashSlot("{}.x"))
    }

    @Test fun `addSlots gan quyen so huu va MOVED logic`() {
        val a = ClusterState("127.0.0.1", 7001)
        val slot = Crc16.keyHashSlot("hello")
        a.addSlots(listOf(slot))
        assertTrue(a.isMine(slot))
        assertEquals(a.myId, a.ownerOf(slot)?.id)
    }

    @Test fun `gossip serialize roi merge sang node khac`() {
        val a = ClusterState("127.0.0.1", 7001)
        a.addSlots((0..100).toList())

        val b = ClusterState("127.0.0.1", 7002)
        b.mergeFrom(a.serialize())                       // b learns about a and the slots a owns

        // b now knows about node a and that slot 50 belongs to a
        assertTrue(b.nodes.containsKey(a.myId))
        assertEquals(a.myId, b.ownerOf(50)?.id)
        assertEquals(false, b.isMine(50))
    }

    @Test fun `CLUSTER SLOTS tra ve array`() {
        val a = ClusterState("127.0.0.1", 7001)
        a.addSlots((0..10).toList())
        val reply = a.slotsReply()
        assertTrue(reply is RespValue.Array)
    }
}
