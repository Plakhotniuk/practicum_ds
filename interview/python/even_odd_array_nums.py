"""
Дан список nums. 
Переместите все чётные числа в начало массива, а все нечётные — в конец. 
Верните получившийся массив. Относительный порядок внутри групп не важен.
Примеры:
nums = [3, 1, 2, 4] → [2, 4, 3, 1];
nums = [0] → [0];
nums = [1, 2, 3, 4] → [2, 4, 1, 3].
"""

from typing import List

def sort_array_by_parity(nums: List[int]) -> List[int]:

    if len(nums) == 1:
        return nums
    i = 0
    for _ in range(len(nums)):

        if nums[i] % 2 != 0:
            tmp = nums.pop(i)
            nums.append(tmp)
        else:
            i+=1

    return nums

test_cases = [
    [3, 1, 2, 4],
    [0],
    [1, 2, 3, 4],
    [2, 4, 6],
    [1, 3, 5],
    [4, 1, 6, 3, 8, 5],
]

for nums in test_cases:
    print(sort_array_by_parity(nums))