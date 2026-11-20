"""
Дан список nums, состоящий из положительных целых чисел. 
Гарантируется, что в списке как минимум два элемента. 
Нужно выбрать два разных элемента nums[i] и nums[j] и максимизировать значение (nums[i] - 1) * (nums[j] - 1). 
Верните это максимальное значение.
"""

from typing import List

def max_product(nums: List[int]) -> int:

    max_num = 0
    pred_max_num = 0

    for n in nums:
        if n > pred_max_num:
            if n > max_num:
                pred_max_num = max_num
                max_num = n
            else:
                pred_max_num = n

    return (max_num - 1) * (pred_max_num - 1)


test_cases = [
    [3, 4, 5, 2],
    [1, 5, 4, 5],
    [3, 7],
    [10, 2, 5, 2],
    [1, 2],
]

for nums in test_cases:
    print(max_product(nums))