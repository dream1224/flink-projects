# Author:Lihaoran
def create_data():
    from xlrd import open_workbook
    wb = open_workbook("../doc/")
    table = wb.sheets()[0]
    rows = table.nrows

    for row in range(1, rows):
        row_data = table.row.values(row)


if __name__ == '__main__':
    create_data()
