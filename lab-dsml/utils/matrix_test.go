package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"gonum.org/v1/gonum/mat"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestSerializeMatrix(t *testing.T) {
	// Test Case 1: Valid Matrix Serialization
	m := mat.NewDense(2, 2, []float64{1.0, 2.0, 3.0, 4.0})
	data, err := SerializeMatrix(m)
	assert.NoError(t, err)
	assert.NotNil(t, data)
}

func TestDeserializeMatrix(t *testing.T) {
	// Test Case 1: Valid Matrix Deserialization
	m := mat.NewDense(2, 2, []float64{1.0, 2.0, 3.0, 4.0})
	data, err := SerializeMatrix(m)
	assert.NoError(t, err)

	deserializedMatrix, err := DeserializeMatrix(data)
	assert.NoError(t, err)
	assert.Equal(t, 2, deserializedMatrix.RawMatrix().Rows)
	assert.Equal(t, 2, deserializedMatrix.RawMatrix().Cols)
	assert.Equal(t, 1.0, deserializedMatrix.At(0, 0))
	assert.Equal(t, 2.0, deserializedMatrix.At(0, 1))
	assert.Equal(t, 3.0, deserializedMatrix.At(1, 0))
	assert.Equal(t, 4.0, deserializedMatrix.At(1, 1))

	// Test Case 2: Invalid Data (wrong size or format)
	_, err = DeserializeMatrix([]byte{1, 2, 3, 4}) // invalid data
	assert.Error(t, err)
}

func TestMaxMatrix(t *testing.T) {
	// Test Case 1: Valid Max Matrix
	a := mat.NewDense(2, 2, []float64{1.0, 2.0, 3.0, 4.0})
	b := mat.NewDense(2, 2, []float64{4.0, 3.0, 2.0, 1.0})
	result, err := MaxMatrix(a, b)
	assert.NoError(t, err)
	assert.Equal(t, 4.0, result.At(0, 0))
	assert.Equal(t, 3.0, result.At(0, 1))
	assert.Equal(t, 3.0, result.At(1, 0))
	assert.Equal(t, 4.0, result.At(1, 1))

	// Test Case 2: Mismatched Dimensions
	c := mat.NewDense(2, 3, []float64{1.0, 2.0, 3.0, 4.0, 5.0, 6.0})
	_, err = MaxMatrix(a, c)
	assert.Error(t, err)
	assert.Equal(t, status.Code(err), codes.InvalidArgument)
}

func TestMinMatrix(t *testing.T) {
	// Test Case 1: Valid Min Matrix
	a := mat.NewDense(2, 2, []float64{1.0, 2.0, 3.0, 4.0})
	b := mat.NewDense(2, 2, []float64{4.0, 3.0, 2.0, 1.0})
	result, err := MinMatrix(a, b)
	assert.NoError(t, err)
	assert.Equal(t, 1.0, result.At(0, 0))
	assert.Equal(t, 2.0, result.At(0, 1))
	assert.Equal(t, 2.0, result.At(1, 0))
	assert.Equal(t, 1.0, result.At(1, 1))

	// Test Case 2: Mismatched Dimensions
	c := mat.NewDense(2, 3, []float64{1.0, 2.0, 3.0, 4.0, 5.0, 6.0})
	_, err = MinMatrix(a, c)
	assert.Error(t, err)
	assert.Equal(t, status.Code(err), codes.InvalidArgument)
}

func TestProdMatrix(t *testing.T) {
	// Test Case 1: Valid Matrix Product
	a := mat.NewDense(2, 3, []float64{1.0, 2.0, 3.0, 4.0, 5.0, 6.0})
	b := mat.NewDense(3, 2, []float64{7.0, 8.0, 9.0, 10.0, 11.0, 12.0})
	result, err := ProdMatrix(a, b)
	assert.NoError(t, err)
	assert.Equal(t, 2, result.RawMatrix().Rows)
	assert.Equal(t, 2, result.RawMatrix().Cols)
	assert.InDelta(t, 58.0, result.At(0, 0), 1e-5)

	// Test Case 2: Incompatible Dimensions
	c := mat.NewDense(2, 3, []float64{1.0, 2.0, 3.0, 4.0, 5.0, 6.0})
	_, err = ProdMatrix(a, c)
	assert.Error(t, err)
	assert.Equal(t, status.Code(err), codes.InvalidArgument)
}

func TestSumMatrix(t *testing.T) {
	// Test Case 1: Valid Sum of Matrices
	a := mat.NewDense(2, 2, []float64{1.0, 2.0, 3.0, 4.0})
	b := mat.NewDense(2, 2, []float64{4.0, 3.0, 2.0, 1.0})
	result, err := SumMatrix(a, b)
	assert.NoError(t, err)
	assert.InDelta(t, 5.0, result.At(0, 0), 1e-5)
	assert.InDelta(t, 5.0, result.At(0, 1), 1e-5)
	assert.InDelta(t, 5.0, result.At(1, 0), 1e-5)
	assert.InDelta(t, 5.0, result.At(1, 1), 1e-5)

	// Test Case 2: Mismatched Dimensions
	c := mat.NewDense(2, 3, []float64{1.0, 2.0, 3.0, 4.0, 5.0, 6.0})
	_, err = SumMatrix(a, c)
	assert.Error(t, err)
	assert.Equal(t, status.Code(err), codes.InvalidArgument)
}

func TestLinearCrossEntropyGradients(t *testing.T) {
	XData := []float64{
		1.0, 3.0, 5.0, 7.0,
		4.0, 6.0, 8.0, 10.0,
	}
	X := mat.NewDense(2, 4, XData)

	// W matrix: 4x3
	WData := []float64{
		1.0, 2.0, 3.0,
		4.0, 5.0, 6.0,
		7.0, 8.0, 9.0,
		10.0, 11.0, 12.0,
	}
	W := mat.NewDense(4, 3, WData)

	// Y matrix: 2x3
	YData := []float64{
		1.0, 0.0, 0.0,
		0.0, 1.0, 0.0,
	}
	Y := mat.NewDense(2, 3, YData)

	expectedGradData := []float64{
		-0.5, -2.0, 2.5,
		-1.5, -3.0, 4.5,
		-2.5, -4.0, 6.5,
		-3.5, -5.0, 8.5,
	}
	expectedGrad := mat.NewDense(4, 3, expectedGradData)

	grad, loss, err := LinearCrossEntropyGradients(X, W, Y)
	assert.Nil(t, err)
	assert.InDelta(t, 30.0, loss, 1e-5)
	for i := range 4 {
		for j := range 3 {
			assert.InDelta(t, expectedGrad.At(i, j), grad.At(i, j), 1e-5)
		}
	}
}

// TestSplitMatrix tests the SplitMatrix function to ensure it splits the matrix correctly into chunks.
func TestSplitMatrix(t *testing.T) {
	// Create a sample matrix
	data := []float64{
		1.0, 2.0, 3.0,
		4.0, 5.0, 6.0,
		7.0, 8.0, 9.0,
		10.0, 11.0, 12.0,
	}
	originalMatrix := mat.NewDense(4, 3, data)

	// Split the matrix into 2 chunks
	chunks := SplitMatrix(originalMatrix, 2)

	// Assert that there are 2 chunks
	assert.Len(t, chunks, 2)

	// Assert that each chunk has the expected number of rows
	assert.Equal(t, 2, chunks[0].RawMatrix().Rows) // First chunk should have 2 rows
	assert.Equal(t, 2, chunks[1].RawMatrix().Rows) // Second chunk should have 2 rows

	// Assert that the number of columns is the same for all chunks
	assert.Equal(t, 3, chunks[0].RawMatrix().Cols)
	assert.Equal(t, 3, chunks[1].RawMatrix().Cols)
}

// TestMergeMatrix tests the MergeMatrix function to ensure it correctly merges gradient matrices.
func TestMergeMatrix(t *testing.T) {
	// Create some sample gradient matrices
	data1 := []float64{
		1.0, 2.0, 3.0,
		4.0, 5.0, 6.0,
	}
	matrix1 := mat.NewDense(2, 3, data1)

	data2 := []float64{
		7.0, 8.0, 9.0,
		10.0, 11.0, 12.0,
	}
	matrix2 := mat.NewDense(2, 3, data2)

	// Merge the matrices
	merged, err := MergeMatrix([]*mat.Dense{matrix1, matrix2})

	// Assert that no error occurred during merging
	assert.NoError(t, err)

	// Assert that the merged matrix has the correct dimensions
	assert.Equal(t, 4, merged.RawMatrix().Rows) // Should have 4 rows (2+2)
	assert.Equal(t, 3, merged.RawMatrix().Cols) // Should have 3 columns

	// Assert that the data matches the original matrices
	expectedData := append(data1, data2...)
	mergedData := merged.RawMatrix().Data
	assert.Equal(t, expectedData, mergedData)
}

// TestSplitMergeConsistency tests if splitting and then merging a matrix results in the original matrix.
func TestSplitMergeConsistency(t *testing.T) {
	// Create a sample matrix
	data := []float64{
		1.0, 2.0, 3.0,
		4.0, 5.0, 6.0,
		7.0, 8.0, 9.0,
		10.0, 11.0, 12.0,
	}
	originalMatrix := mat.NewDense(4, 3, data)

	// Split the matrix into 2 chunks
	chunks := SplitMatrix(originalMatrix, 2)

	// Merge the chunks back together
	merged, err := MergeMatrix(chunks)

	// Assert no error occurred during merging
	assert.NoError(t, err)

	// Assert that the merged matrix is the same size as the original matrix
	rOrig, cOrig := originalMatrix.Dims()
	rMerged, cMerged := merged.Dims()

	assert.Equal(t, rOrig, rMerged)
	assert.Equal(t, cOrig, cMerged)

	// Assert that the merged matrix data is equal to the original matrix data
	originalData := originalMatrix.RawMatrix().Data
	mergedData := merged.RawMatrix().Data
	assert.Equal(t, originalData, mergedData)
}
