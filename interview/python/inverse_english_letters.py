"""
Дана строка s. 
Нужно развернуть только буквы английского алфавита, а все остальные символы оставить на своих местах. 

Примеры
s = "ab-cd" → "dc-ba";
s = "a-bC-dEf-ghIj" → "j-Ih-gfE-dCba";
s = "Test1ng-Leet=code-Q!" → "Qedo1ct-eeLg=ntse-T!".

Ожидаемая сложность
Время: O(n).
Дополнительная память: O(n) в Python, если переводить строку в список символов.
"""
import string

def reverse_only_letters(s: str) -> str:

    eng_letters = string.ascii_letters

    if len(s) < 2:
        return s

    s_list = list(s)

    left_idx = 0
    right_idx = len(s) - 1

    while left_idx < right_idx:

        left_letter = s_list[left_idx]
        right_letter = s_list[right_idx]

        if left_letter in eng_letters and right_letter in eng_letters:
            s_list[left_idx] = right_letter
            s_list[right_idx] = left_letter
            left_idx += 1
            right_idx -= 1

        if not left_letter in eng_letters:
            left_idx += 1

        if not right_letter in eng_letters:
            right_idx -= 1

    return ''.join(s_list)


test_cases = [
    "ab-cd",
    "a-bC-dEf-ghIj",
    "Test1ng-Leet=code-Q!",
    "7_28]",
    "A",
]

for s in test_cases:
    print(reverse_only_letters(s))