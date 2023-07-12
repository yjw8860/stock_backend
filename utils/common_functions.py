
from bisect import bisect_left
import json
import itertools

def loadJson(path):
    with open(path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    return data

def flatten_with_itertools(a):
    """
    a: 2차원 리스트
    return: 1차원 리스트
    """
    return list(itertools.chain(*a))

def find_nearest(a, b):
    """
    a: integer가 여러개 있는 list
    b: integer
    return: a의 원소 중 b와 가장 가까운 원소의 index 반환
            만약, b와 가장 가까운 원소가 2개라면, b보다 작은 원소의 index를 반환
    """
    idx = bisect_left(a, b)

    # b가 a의 모든 원소보다 작은 경우
    if idx == 0:
        return idx
    # b가 a의 모든 원소보다 큰 경우
    elif idx == len(a):
        return idx - 1
    # b와 가장 가까운 a의 원소가 2개인 경우 (b보다 작은 원소의 index를 반환)
    elif a[idx] - b == b - a[idx - 1]:
        return idx - 1
    # b와 가장 가까운 a의 원소가 1개인 경우
    elif a[idx] - b < b - a[idx - 1]:
        return idx
    else:
        return idx - 1