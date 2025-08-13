# Example Python file with various technical debt issues for testing

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
    
    def very_long_method_with_many_lines(self, param1, param2, param3):
        """This method is intentionally long to trigger the long method detector"""
        result = []
        
        # Line 1
        if param1 is not None:
            result.append(param1)
        
        # Line 5
        if param2 is not None:
            result.append(param2)
        
        # Line 9
        if param3 is not None:
            result.append(param3)
        
        # Line 13
        for i in range(10):
            if i % 2 == 0:
                result.append(i * 2)
            else:
                result.append(i * 3)
        
        # Line 20
        for item in result:
            if item > 10:
                self.data.append(item)
            elif item > 5:
                self.cache[str(item)] = item
            else:
                self.results.append(item)
        
        # Line 28
        if len(self.data) > 0:
            self.count += len(self.data)
        
        # Line 32
        if len(self.cache) > 0:
            self.status = "cached"
        
        # Line 36
        if len(self.results) > 0:
            self.status = "processed"
        
        # Line 40
        for key, value in self.cache.items():
            if value > 15:
                self.errors.append(f"Value too high: {value}")
            elif value > 10:
                self.warnings.append(f"Value high: {value}")
            else:
                self.info.append(f"Value normal: {value}")
        
        # Line 48
        return result
    
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

def another_unused_function(param):
    """Another unused function"""
    return param * 2

def main():
    """Main function that uses the class"""
    example = LargeExampleClass()
    result = example.very_long_method_with_many_lines(1, 2, 3)
    complex_result = example.complex_method_with_many_branches(5, 3, 1)
    
    print(f"Result: {result}")
    print(f"Complex result: {complex_result}")

if __name__ == "__main__":
    main()
