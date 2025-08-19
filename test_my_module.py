import pytest
from my_module import add, subtract

def test_add_positive_numbers():
    """Tests the add function with positive numbers."""
    assert add(2, 3) == 5

def test_subtract_positive_numbers():
    """Tests the subtract function with positive numbers."""
    assert subtract(5, 2) == 3

def test_add_negative_numbers():
    """Tests the add function with negative numbers."""
    assert add(-1, -4) == -5

def test_subtract_zero():
    """Tests the subtract function when subtracting zero."""
    assert subtract(7, 0) == 7
