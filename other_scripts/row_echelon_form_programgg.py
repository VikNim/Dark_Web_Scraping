column_index = list()
column_check = row_pivot = False
matrix = [[1, 9, -4, 0], [0, 2, 3, 6], [2, 0, 1, 0], [0, 0, 0, 4]]
# matrix2 = [[0, 0, 0]]
# matrix = [[0, 0, 0], [0, 1, 0]]
# matrix1 = [[0, 0, 0, 0, 1], [0, 0, 1, 0, 0]]

pivot = list()
print(matrix)

def exchange_rows(i):
	print(i)
	temp_list = matrix[i]
	matrix[i] = matrix[pivot[-1]]
	matrix[pivot[-1]] = temp_list
	return


for i in range(len(matrix)):
	row_pivot = False
	for j in range(len(matrix[i])):
		if row_pivot:
			break
		if matrix[i][j] is not 0:
			print('We have found a pivot..!')
			if row_pivot:
				print('Pivot already exists in row..!')
				break
			else:
				row_pivot = True
				for k in column_index:
					if j is k or j < k:
						# column_check = True
						exchange_rows(i)
						column_check = True
						break
				if column_check:
				    print('The pivot already exists in same column..')
				    print('Need to do Row Exchange..!')
				    break
				else:
					column_index.append(j)
					pivot.append(i)
print('Column Index:', column_index)
print('Pivots List:', pivot)
print('Matrix List:', matrix)
