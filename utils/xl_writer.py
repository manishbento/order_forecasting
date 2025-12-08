"""
Excel Writer Utility
====================
Wrapper around xlsxwriter for creating formatted Excel reports.

Provides consistent formatting and helper methods for Excel export.
"""

import xlsxwriter


class XLWriter:
    """
    Excel writer with predefined formats and helper methods.
    
    Usage:
        xl = XLWriter('output.xlsx')
        ws = xl.wb.add_worksheet('Data')
        xl.write_data_header(ws, 0, ['Col1', 'Col2'])
        xl.wb.close()
    """
    
    # Font settings
    DEFAULT_FONT = 'Aptos Narrow'
    HEADER_1_SIZE = 14
    HEADER_2_SIZE = 12
    HEADER_3_SIZE = 11
    
    # Format definitions
    FORMAT_TEXT = {'num_format': '@', 'font_name': DEFAULT_FONT}
    FORMAT_NUM_NON_SCIENTIFIC = {'num_format': '0', 'font_name': DEFAULT_FONT}
    FORMAT_XLSX_DATE = {'num_format': 'yyyy-mm-dd', 'font_name': DEFAULT_FONT, 'align': 'center'}
    FORMAT_XLSX_DATE_COSTCO = {'num_format': 'm/d/yyyy', 'font_name': DEFAULT_FONT, 'align': 'center'}
    FORMAT_NUM_NODECIMAL = {'num_format': '#,##0', 'font_name': DEFAULT_FONT}
    FORMAT_NUM_CURRENCY = {'num_format': '$#,##0.00'}
    FORMAT_NUM_CURRENCY_TOTAL = {'num_format': '$#,##0.00', 'bold': True}
    FORMAT_NUM_DECIMAL = {'num_format': '#,##0.00', 'font_name': DEFAULT_FONT}
    FORMAT_NUM_DECIMAL_TOTAL = {'num_format': '#,##0.00', 'font_name': DEFAULT_FONT, 'bold': True}
    FORMAT_NUM_PERCENTAGE = {'num_format': '0.00%', 'font_name': DEFAULT_FONT}
    FORMAT_NUM_PERCENTAGE_SINGLE = {'num_format': '0.0%', 'font_name': DEFAULT_FONT}
    FORMAT_NUM_NODECIMAL_TOTAL = {'num_format': '#,##0', 'font_name': DEFAULT_FONT, 'bold': True}
    FORMAT_NUM_PERCENTAGE_TOTAL = {'num_format': '0.00%', 'font_name': DEFAULT_FONT, 'bold': True}
    FORMAT_NUM_PERCENTAGE_SINGLE_TOTAL = {'num_format': '0.0%', 'font_name': DEFAULT_FONT, 'bold': True}
    FORMAT_HEADER = {'font_name': DEFAULT_FONT, 'font_size': HEADER_1_SIZE, 'bold': True}
    FORMAT_HEADER_2 = {'font_name': DEFAULT_FONT, 'font_size': HEADER_2_SIZE, 'bold': True}
    FORMAT_HEADER_3 = {'font_name': DEFAULT_FONT, 'font_size': HEADER_3_SIZE}
    FORMAT_BOLD = {'font_name': DEFAULT_FONT, 'bold': True}
    FORMAT_COL_TITLE = {
        'font_name': DEFAULT_FONT, 'bold': True, 'align': 'center',
        'font_size': 11, 'bg_color': '#E5E4E2', 'text_wrap': True
    }
    FORMAT_CENTER = {'font_name': DEFAULT_FONT, 'align': 'center'}
    
    def __init__(self, filename: str = None):
        """
        Initialize Excel writer.
        
        Args:
            filename: Output file path
        """
        self.wb = xlsxwriter.Workbook(filename, {'strings_to_numbers': True})
        self._apply_default_format()
    
    @staticmethod
    def num_to_char(num: int) -> str:
        """Convert column number to letter (0='A', 1='B', etc.)."""
        return xlsxwriter.utility.xl_col_to_name(num)
    
    def _apply_default_format(self):
        """Set up all standard formats."""
        self.wb.formats[0].set_font_name(self.DEFAULT_FONT)
        
        self.format_date = self.wb.add_format(self.FORMAT_XLSX_DATE)
        self.format_date_costco = self.wb.add_format(self.FORMAT_XLSX_DATE_COSTCO)
        self.format_num = self.wb.add_format(self.FORMAT_NUM_NODECIMAL)
        self.format_num_total = self.wb.add_format(self.FORMAT_NUM_NODECIMAL_TOTAL)
        self.format_num_currency = self.wb.add_format(self.FORMAT_NUM_CURRENCY)
        self.format_num_currency_total = self.wb.add_format(self.FORMAT_NUM_CURRENCY_TOTAL)
        self.format_num_decimal = self.wb.add_format(self.FORMAT_NUM_DECIMAL)
        self.format_num_decimal_total = self.wb.add_format(self.FORMAT_NUM_DECIMAL_TOTAL)
        self.format_header = self.wb.add_format(self.FORMAT_HEADER)
        self.format_header_2 = self.wb.add_format(self.FORMAT_HEADER_2)
        self.format_bold = self.wb.add_format(self.FORMAT_BOLD)
        self.format_col_title = self.wb.add_format(self.FORMAT_COL_TITLE)
        self.format_center = self.wb.add_format(self.FORMAT_CENTER)
        self.format_percentage = self.wb.add_format(self.FORMAT_NUM_PERCENTAGE)
        self.format_percentage_total = self.wb.add_format(self.FORMAT_NUM_PERCENTAGE_TOTAL)
        self.format_percentage_single = self.wb.add_format(self.FORMAT_NUM_PERCENTAGE_SINGLE)
        self.format_percentage_single_total = self.wb.add_format(self.FORMAT_NUM_PERCENTAGE_SINGLE_TOTAL)
        self.format_text = self.wb.add_format(self.FORMAT_TEXT)
        self.format_num_non_scientific = self.wb.add_format(self.FORMAT_NUM_NON_SCIENTIFIC)
    
    def write_data_header(self, ws, row: int, columns: list, 
                          start: int = 0, format_cols: bool = True, formats: dict = None):
        """
        Write column headers with formatting.
        
        Args:
            ws: Worksheet object
            row: Row number for headers
            columns: List of column names
            start: Starting column index
            format_cols: Whether to auto-format columns by name
            formats: Custom format overrides by column name
            
        Returns:
            Worksheet with headers written
        """
        # Write headers
        for ix, col in enumerate(columns):
            ws.write(row, start + ix, col, self.format_col_title)
        
        # Auto-format columns based on name
        if format_cols:
            for i, col in enumerate(columns):
                col_lower = col.lower()
                cn = self.num_to_char(i)
                
                if 'date' in col_lower:
                    ws.set_column(f'{cn}:{cn}', 18, self.format_date)
                elif 'amount' in col_lower or 'diff' in col_lower:
                    ws.set_column(f'{cn}:{cn}', 18, self.format_num_decimal)
                elif 'quantity' in col_lower:
                    ws.set_column(f'{cn}:{cn}', 18, self.format_num)
                elif 'percent' in col_lower:
                    ws.set_column(f'{cn}:{cn}', 18, self.format_percentage_single)
        
        # Apply custom formats
        if formats:
            for i, col in enumerate(columns):
                if col in formats:
                    cn = self.num_to_char(i)
                    ws.set_column(f'{cn}:{cn}', 18, formats[col])
        
        # Freeze header row
        ws.freeze_panes(f"A{row + 2}")
        
        return ws
    
    def set_header(self, ws, header1: str, header2: str = None, header3: str = None):
        """
        Write report headers.
        
        Args:
            ws: Worksheet object
            header1: Main header text
            header2: Secondary header text
            header3: Tertiary header text
            
        Returns:
            Worksheet with headers written
        """
        ws.write(0, 0, header1, self.format_header)
        if header2:
            ws.write(1, 0, header2, self.format_header_2)
        if header3:
            ws.write(2, 0, header3, self.format_bold)
        return ws
    
    def db_to_worksheet(self, columns: list, data: list, ws_name: str = 'Data',
                        row: int = 0, formats: dict = None):
        """
        Create worksheet from database results.
        
        Args:
            columns: List of column names
            data: List of row tuples
            ws_name: Worksheet name
            row: Starting row
            formats: Custom format overrides
            
        Returns:
            Created worksheet
        """
        ws = self.wb.add_worksheet(ws_name)
        ws = self.write_data_header(ws, row, columns, formats=formats)
        
        i = 0
        j = 0
        for i, d in enumerate(data, 1):
            for j, f in enumerate(d):
                if formats and columns[j] in formats:
                    if formats[columns[j]] == self.format_text:
                        ws.write_string(row + i, j, f or "")
                    else:
                        ws.write(row + i, j, f)
                else:
                    ws.write(row + i, j, f)
        
        ws.autofilter(row, 0, row + i - 1, j)
        return ws
    
    def close(self):
        """Close the workbook."""
        self.wb.close()
