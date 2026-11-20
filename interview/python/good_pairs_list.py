"""
Дан список nums. 
Хорошей называется пара индексов (i, j), если i < j и nums[i] == nums[j]. 
Нужно вернуть количество хороших пар.

Примеры:
nums = [1, 2, 3, 1, 1, 3] → 4;
nums = [1, 1, 1, 1] → 6;
nums = [1, 2, 3] → 0.
Ожидаемая сложность
Время: O(n).
Дополнительная память: O(n).
"""
from typing import List
from math import factorial

def num_identical_pairs(nums: List[int]) -> int:
    
    nums_idx_dict = dict()
    good_pairs_counter = 0

    for num in nums:
        nums_idx_dict[num] = nums_idx_dict.get(num, 0) + 1

    for key, value in nums_idx_dict.items():
        if value > 1:
            good_pairs_counter += factorial(value) / (factorial(value - 2) * 2)

    return int(good_pairs_counter)


test_cases = [
    [1, 2, 3, 1, 1, 3],
    [1, 1, 1, 1],
    [1, 2, 3],
    [5],
    [2, 2, 2, 2, 2],
]

for nums in test_cases:
    print(num_identical_pairs(nums))