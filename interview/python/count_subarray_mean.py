"""
Дан список arr, а также числа k и threshold. 
Нужно вернуть количество подмассивов длины k, у которых среднее значение больше или равно threshold. 

Примеры
arr = [2, 2, 2, 2, 5, 5, 5, 8], k = 3, threshold = 4 → 3;
arr = [11, 13, 17, 23, 29, 31, 7, 5, 2, 3], k = 3, threshold = 5 → 6;
arr = [1, 1, 1, 1, 1], k = 1, threshold = 1 → 5.
Ожидаемая сложность
Время: O(n).
Дополнительная память: O(1).
"""

from typing import List

def num_of_subarrays(arr: List[int], k: int, threshold: int) -> int:

    # обработка краевых случаев
    if len(arr) < k:
        return -1

    res_counter = 0
    tmp_sum = 0

    # сумма первого подмассива
    for i in range(k):
        tmp_sum += arr[i]

    res_counter = 1 if tmp_sum / k >= threshold else 0
    # расчет сумм в скользящем окну
    for i in range(k, len(arr)):
        tmp_sum += arr[i]
        tmp_sum -= arr[i - k]

        if tmp_sum / k >= threshold:
            res_counter += 1

    return res_counter


test_cases = [
    ([2, 2, 2, 2, 5, 5, 5, 8], 3, 4),
    ([11, 13, 17, 23, 29, 31, 7, 5, 2, 3], 3, 5),
    ([1, 1, 1, 1, 1], 1, 1),
    ([5, 5, 5, 5], 4, 5),
    ([1, 2, 3, 4], 2, 10),
]

for arr, k, threshold in test_cases:
    print(num_of_subarrays(arr, k, threshold))