package io.ktredis.command

/**
 * Matches a string against a Redis-style glob pattern:
 *   *        matches 0 or more characters
 *   ?        matches exactly 1 character
 *   [abc]    matches 1 character in the set
 *   [a-z]    matches 1 character in the range
 *   [^...]   negation
 *   \x       literal character x
 */
fun globMatch(pattern: String, str: String): Boolean = match(pattern, 0, str, 0)

private fun match(p: String, pi0: Int, s: String, si0: Int): Boolean {
    var pi = pi0
    var si = si0
    while (pi < p.length) {
        when (p[pi]) {
            '*' -> {
                while (pi + 1 < p.length && p[pi + 1] == '*') pi++   // collapse '**'
                if (pi + 1 == p.length) return true                  // trailing '*' matches the rest
                for (k in si..s.length) if (match(p, pi + 1, s, k)) return true
                return false
            }
            '?' -> {
                if (si >= s.length) return false
                pi++; si++
            }
            '[' -> {
                if (si >= s.length) return false
                var j = pi + 1
                val negate = j < p.length && p[j] == '^'
                if (negate) j++
                var matched = false
                while (j < p.length && p[j] != ']') {
                    if (j + 2 < p.length && p[j + 1] == '-' && p[j + 2] != ']') {
                        if (s[si] in p[j]..p[j + 2]) matched = true
                        j += 3
                    } else {
                        if (p[j] == s[si]) matched = true
                        j++
                    }
                }
                if (j < p.length && p[j] == ']') j++
                if (matched == negate) return false
                pi = j; si++
            }
            '\\' -> {
                if (pi + 1 < p.length) pi++
                if (si >= s.length || s[si] != p[pi]) return false
                pi++; si++
            }
            else -> {
                if (si >= s.length || s[si] != p[pi]) return false
                pi++; si++
            }
        }
    }
    return si == s.length
}
