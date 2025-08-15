++ b/example_single_file.py

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
        
        # Validate and collect parameters
        result = self._validate_and_collect_params(param1, param2, param3)
        
        # Process number range
        result.extend(self._process_number_range())
        
        # Categorize results
        self._categorize_results(result)
        
        # Update status and process cache
        self._update_status_and_process_cache()
        
        return result
    
    def _validate_and_collect_params(self, param1, param2, param3):
        """Validate and collect non-None parameters"""
        result = []
        for param in [param1, param2, param3]:
            if param is not None:
                result.append(param)
        return result
    
    def _process_number_range(self):
        """Process number range with even/odd logic"""
        processed = []
        for i in range(10):
            if i % 2 == 0:
                processed.append(i * 2)
            else:
                processed.append(i * 3)
        return processed
    
    def _categorize_results(self, result):
        """Categorize results into data, cache, and results based on value"""
        for item in result:
            if item > 10:
                self.data.append(item)
            elif item > 5:
                self.cache[str(item)] = item
            else:
                self.results.append(item)
    
    def _update_status_and_process_cache(self):
        """Update status based on data and results, and process cache values"""
        # Update status based on collections
        if len(self.data) > 0:
            self.count += len(self.data)
        
        if len(self.cache) > 0:
            self.status = "cached"
        
        if len(self.results) > 0:
            self.status = "processed"
        
        # Process cache values and categorize messages
        for key, value in self.cache.items():
            if value > 15:
                self.errors.append(f"Value too high: {value}")
            elif value > 10:
                self.warnings.append(f"Value high: {value}")
            else:
                self.info.append(f"Value normal: {value}")
    
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
