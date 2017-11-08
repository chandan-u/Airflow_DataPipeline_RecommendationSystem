import unittest
 
import os
import subprocess

class TestTransformation(unittest.TestCase):
    """
    Test the Transformation process
    """
 
    def test_check_row_count_facttable(self):
        """
            test join result
            No of rows in result table should be <= number of rows in source tables of join
            should pass
        """


        
        # airflow server should be running 
        datasets_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),'..','datasets')
        
        # total lines transformed fact file
        number_lines = 0
        for filename in os.listdir(os.path.join(datasets_path, 'fact_movileLens.csv', '')):
            if filename.endswith(".csv"): 
                print(filename)
                file = open( os.path.join(datasets_path, 'fact_movileLens.csv', filename))
                number_lines += len(file.readlines())
                file.close()

        
        print(number_lines)
        
        # total lines in induviudal ratings files 
        actual_number_lines = 0
        filename = os.path.join(datasets_path, 'ratings.csv')
        file = open(filename)
        actual_number_lines = len(file.readlines())
        print(filename)
        print(actual_number_lines)
        file.close() 

 
        # for innter join : no of rows in result table should be <= parent table   
        # must account for headers in each file (check if spark generates headers)
        assert actual_number_lines >= number_lines



    def test_check_python2(self):
        """
            should fail: urllib package is different
            yet to write
        """
        pass

    def test_check_python3_6(self):
        """
            should fail: Spark doesnt support python3.6
            yet to write
        """
        pass


if __name__ == '__main__':
    unittest.main()





