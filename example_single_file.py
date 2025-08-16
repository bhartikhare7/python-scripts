class LargeExampleClass:
    """This class demonstrates multiple technical debt issues"""
    
    def __init__(self):
        self.data = []
        self.count = 0
        self.status = "active"
        self.config = {}
        self.cache = {}
        self.results = []
        self.errors = []
        self.warnings = []
        self.info = []
        self.debug = []
    
    def complex_method_with_many_branches(self, x, y, z):
        """This method has high cyclomatic complexity"""
        if x > 0:
            if y > 0:
                if z > 0:
                    if x > y:
                        if y > z:
                            if x > z:
                                return x + y + z
                            else:
                                return x + y - z
                        else:
                            return x - y + z
                    else:
                        return y + z
                else:
                    return x + y
            else:
                return x
        else:
            return 0
    
    def duplicate_code_block_one(self):
        """First instance of duplicate code"""
        data = []
        for i in range(5):
            if i % 2 == 0:
                data.append(i * 2)
            else:
                data.append(i * 3)
        return data
    
    def duplicate_code_block_two(self):
        """Second instance of duplicate code"""
        data = []
        for i in range(5):
            if i % 2 == 0:
                data.append(i * 2)
            else:
                data.append(i * 3)
        return data

def unused_function():
    """This function is never called - dead code"""
    return "This function is unused"

def main():
    """Main function that uses the class"""
    example = LargeExampleClass()
    result = example.very_long_method_with_many_lines(1, 2, 3)
    complex_result = example.complex_method_with_many_branches(5, 3, 1)
    
    print(f"Result: {result}")
    print(f"Complex result: {complex_result}")

if __name__ == "__main__":
    main()
