"""
Дан список различных положительных чисел nums. 
Нужно вернуть любой подходящий элемент, который не является ни минимальным, ни максимальным. 
Не требуется находить все такие элементы или выбирать один «особый». 
Если подходит несколько значений, можно вернуть любое. 
Если подходящего элемента нет, верните -1.
"""

from typing import List

def find_non_min_or_max(nums: List[int]) -> int:
    
    if len(nums) < 3:
        return -1

    if nums[0] > nums[1]:
        max_el = nums[0]
        min_el = nums[1]
    else:
        max_el = nums[1]
        min_el = nums[0]

    
    el = nums[2]
    if el > max_el:
        return max_el
    elif el < min_el:
        return min_el
    else:
        return el



test_cases = [
    [3, 2, 1, 4],
    [1, 2],
    [2, 1, 3],
    [5, 1, 3],
    [10, 4, 7, 1, 12],
]

for nums in test_cases:
    print(find_non_min_or_max(nums))