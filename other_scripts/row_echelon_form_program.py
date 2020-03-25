# We maintain two lists - one for Row and one for column.
pivot = column_index = list()
row_pivot = False
# matrix = [[1, 9, -4, 0], [0, 2, 3, 6], [2, 0, 1, 0], [0, 0, 0, 4]]
# matrix2 = [[0, 0, 0]]
# matrix = [[0, 0, 0], [0, 1, 0]]
matrix = [[0, 0, 0, 0, 1], [0, 0, 1, 0, 0],[0, 1, 0, 0, 0]]

def exchange_rows(i):
	# Exchanging Rows
	temp_list = matrix[i]
	matrix[i] = matrix[pivot[-1]]
	matrix[pivot[-1]] = temp_list
	return

# Beginning of logic to check pivot
for i in range(len(matrix)):
	# If row_pivot becomes true, we break out of loop.
	row_pivot = False
	for j in range(len(matrix[i])):
		if row_pivot:
			break
		if matrix[i][j] is not 0:
			# When we find none zero element in row.
			print('We have found a pivot..!')
			if row_pivot:
				# If pivot is already been found, no need to go further, we break out of loop.
				print('Pivot already exists in row..!')
				break
			else:
				# When we find pivot assign True to row_pivot that is not already exist in same column and row
				row_pivot = True
				# Here we check if do we have pivot in same column also.
				for k in column_index:
					if j is k or j < k:
						# if pivot is already present in same coulum, we go for Row Exchange
						exchange_rows(i)
						break
				else:
					column_index.append(j)
					pivot.append(i)

print('Column Index:', column_index)
print('Pivots List:', pivot)
print('Matrix List:', matrix)