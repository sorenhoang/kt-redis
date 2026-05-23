package io.ktredis.cluster

/**
 * CRC16-CCITT (XMODEM): poly 0x1021, init 0x0000, không reflect — đúng bản Redis dùng cho hash slot.
 * Check value: CRC16("123456789") == 0x31C3.
 */
object Crc16 {
    const val SLOTS = 16384

    private val table = IntArray(256).also { t ->
        for (i in 0 until 256) {
            var crc = i shl 8
            repeat(8) {
                crc = if (crc and 0x8000 != 0) (crc shl 1) xor 0x1021 else crc shl 1
            }
            t[i] = crc and 0xFFFF
        }
    }

    fun crc16(data: ByteArray): Int {
        var crc = 0
        for (b in data) {
            crc = ((crc shl 8) xor table[((crc ushr 8) xor (b.toInt() and 0xFF)) and 0xFF]) and 0xFFFF
        }
        return crc
    }

    /** slot của key, có xử lý hash tag {...}: nếu có {tag} không rỗng thì chỉ hash phần trong ngoặc. */
    fun keyHashSlot(key: String): Int {
        val s = key.indexOf('{')
        if (s >= 0) {
            val e = key.indexOf('}', s + 1)
            if (e > s + 1) {                                   // có nội dung không rỗng giữa { và }
                return crc16(key.substring(s + 1, e).toByteArray(Charsets.UTF_8)) % SLOTS
            }
        }
        return crc16(key.toByteArray(Charsets.UTF_8)) % SLOTS
    }
}
