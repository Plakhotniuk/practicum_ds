"""
Дан список слов words и строка chars. 
Слово можно собрать, если для каждого его символа в chars хватает соответствующих букв. 
Нужно вернуть сумму длин всех слов, которые можно собрать.

Примеры:
words = ["cat", "bt", "hat", "tree"], chars = "atach" → 6;
words = ["hello", "world", "leetcode"], chars = "welldonehoneyr" → 10;
words = ["a", "b", "ab", "abc"], chars = "ab" → 4.
Ожидаемая сложность
Время: O(total_chars).
Дополнительная память: O(1) для фиксированного алфавита — или O(k) для числа разных символов.
"""

from typing import List

def count_characters(words: List[str], chars: str) -> int:
    total_length = 0

    # Создаем базовый словарь частот для chars
    chars_dict = {}
    for c in chars:
        chars_dict[c] = chars_dict.get(c, 0) + 1

    for word in words:
        # Быстрая проверка
        word_dict = {}
        can_build = True

        for c in word:
            word_dict[c] = word_dict.get(c, 0) + 1
            # Если такой буквы вообще нет или её уже нужно больше, чем есть в пуле
            if c not in chars_dict or word_dict[c] > chars_dict[c]:
                can_build = False
                break  # Прерываем проверку этого слова раньше времени

        if can_build:
            total_length += len(word)

    return total_length

test_cases = [
    (["cat", "bt", "hat", "tree"], "atach"),
    (["hello", "world", "leetcode"], "welldonehoneyr"),
    (["a", "b", "ab", "abc"], "ab"),
    ([], "abc"),
    (["aa", "aaa", "aaaa"], "aaa"),
]

for words, chars in test_cases:
    print(count_characters(words, chars))
