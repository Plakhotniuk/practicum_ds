"""
Дан список nums. 
Для каждого индекса i нужно посчитать модуль разницы между суммой элементов слева от i и суммой элементов справа от i. 
Верните список таких значений. Это задача на префиксные суммы (prefix sum). 
Основа — LeetCode Left and Right Sum Differences.

Примеры
nums = [10, 4, 8, 3] → [15, 1, 11, 22];
nums = [1] → [0];
nums = [1, 2, 3, 4] → [9, 6, 1, 6].
Ожидаемая сложность
Время: O(n).
Дополнительная память: O(n) на массив ответа — или O(1) дополнительной памяти, если не считать сам ответ.
"""

from typing import List

def left_right_difference(nums: List[int]) -> List[int]:

    nums_len = len(nums)

    if nums_len == 0:
        return []
    
    # краевые случаи
    if nums_len == 1:
        return [0]

    # создание массива кумулятивных сумм

    sum_list = [0]

    for i in range(nums_len):
        sum_list.append(sum_list[i]+nums[i])

    result = []
    for i in range(nums_len):
        result.append(
            abs(sum_list[i] - (sum_list[nums_len] - sum_list[i+1]))
        )

    return result
    

test_cases = [
    [10, 4, 8, 3],
    [1],
    [1, 2, 3, 4],
    [5, 5, 5],
    [3, 1],
]

for nums in test_cases:
    print(left_right_difference(nums))