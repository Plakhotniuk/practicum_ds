"""
Дана строка s, содержащая римское число. Нужно преобразовать её в обычное целое число.
Примеры:
s = "III" → 3;
s = "LVIII" → 58;
s = "MCMXCIV" → 1994.
"""
def roman_to_int(s: str) -> int:

    if len(s) == 0:
        return -1

    convertions_map = {
        'I': 1,
        'V': 5,
        'X': 10,
        'L': 50,
        'C': 100,
        'D': 500,
        'M': 1000
    }
    sign_flag = 1
    prev_num = convertions_map[s[0]]
    letter_counter = 0
    result = 0
    
    for letter in s:
        num = convertions_map[letter]
        if prev_num != num:
            sign_flag = -1 if num > prev_num else 1
            result += letter_counter * prev_num * sign_flag
            letter_counter = 1
        else:
            letter_counter += 1
        prev_num = num

    sign_flag = -1 if convertions_map[s[-1]] > prev_num else 1
    result += letter_counter * prev_num * sign_flag

    return result

test_cases = [
    "III",
    "LVIII",
    "MCMXCIV",
    "IV",
    "IX",
    "XLII",
]

for s in test_cases:
    print(roman_to_int(s))