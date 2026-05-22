package io.ktredis.storage

sealed interface RedisObject {
    class StringValue(var data: ByteArray) : RedisObject
    class ListValue(val items: ArrayDeque<ByteArray> = ArrayDeque()) : RedisObject
    class HashValue(val fields: LinkedHashMap<String, ByteArray> = LinkedHashMap()) : RedisObject
    class SetValue(val members: LinkedHashSet<String> = LinkedHashSet()) : RedisObject

    class SortedSetValue : RedisObject {
        private val scores = HashMap<String, Double>()                 // member -> score (tra O(1))
        private val order = java.util.TreeSet(comparator)              // (score, member) đã sắp xếp

        /** true nếu là member mới. */
        fun add(member: String, score: Double): Boolean {
            val old = scores.put(member, score)
            if (old != null) order.remove(old to member)               // gỡ entry cũ trước
            order.add(score to member)
            return old == null
        }
        fun incrBy(member: String, delta: Double): Double {
            val next = (scores[member] ?: 0.0) + delta
            add(member, next)
            return next
        }
        fun remove(member: String): Boolean {
            val old = scores.remove(member) ?: return false
            order.remove(old to member)
            return true
        }
        fun score(member: String): Double? = scores[member]
        val size: Int get() = scores.size
        /** rank 0-based theo thứ tự tăng, hoặc null nếu không có member. */
        fun rank(member: String): Int? {
            val s = scores[member] ?: return null
            return order.headSet(s to member).size                     // số phần tử nhỏ hơn
        }
        fun ascending(): List<Pair<Double, String>> = order.toList()

        companion object {
            private val comparator = Comparator<Pair<Double, String>> { a, b ->
                val c = a.first.compareTo(b.first)                     // theo score trước
                if (c != 0) c else a.second.compareTo(b.second)        // bằng điểm -> theo member
            }
        }
    }
}