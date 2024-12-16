package server_lib

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"

	"gonum.org/v1/gonum/mat"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func SerializeMatrix(m *mat.Dense) ([]byte, error) {
	raw := m.RawMatrix()
	buf := new(bytes.Buffer)

	// row and col dimensions
	err := binary.Write(buf, binary.LittleEndian, int32(raw.Rows))
	if err != nil {
		return nil, err
	}
	err = binary.Write(buf, binary.LittleEndian, int32(raw.Cols))
	if err != nil {
		return nil, err
	}

	// the data itself
	for _, v := range raw.Data {
		err := binary.Write(buf, binary.LittleEndian, v)
		if err != nil {
			return nil, err
		}
	}

	return buf.Bytes(), nil
}

func DeserializeMatrix(data []byte) (*mat.Dense, error) {
	buf := bytes.NewReader(data)

	var rows, cols int32
	// dimensions (rows and cols)
	err := binary.Read(buf, binary.LittleEndian, &rows)
	if err != nil {
		return nil, err
	}
	err = binary.Read(buf, binary.LittleEndian, &cols)
	if err != nil {
		return nil, err
	}

	// the data itself
	matrixData := make([]float64, rows*cols)
	for i := range matrixData {
		err := binary.Read(buf, binary.LittleEndian, &matrixData[i])
		if err != nil {
			return nil, err
		}
	}

	// reconstruct the Dense matrix
	return mat.NewDense(int(rows), int(cols), matrixData), nil
}

func NumBytesAfterSerialize(r int, c int) int {
	return 8 + (r * c * 8)
}

func MaxMatrix(a *mat.Dense, b *mat.Dense) (*mat.Dense, error) {
	rA, cA := a.Dims()
	rB, cB := b.Dims()
	if rA != rB || cA != cB {
		return nil, status.Errorf(codes.InvalidArgument, "matrices do not have the same dimension")
	}
	result := mat.NewDense(rA, cA, nil)
	for i := 0; i < rA; i++ {
		for j := 0; j < cA; j++ {
			maxVal := max(a.At(i, j), b.At(i, j))
			result.Set(i, j, maxVal)
		}
	}
	return result, nil
}

func MinMatrix(a *mat.Dense, b *mat.Dense) (*mat.Dense, error) {
	rA, cA := a.Dims()
	rB, cB := b.Dims()
	if rA != rB || cA != cB {
		return nil, status.Errorf(codes.InvalidArgument, "matrices do not have the same dimension")
	}
	result := mat.NewDense(rA, cA, nil)
	for i := 0; i < rA; i++ {
		for j := 0; j < cA; j++ {
			minVal := min(a.At(i, j), b.At(i, j))
			result.Set(i, j, minVal)
		}
	}
	return result, nil
}

func ProdMatrix(a *mat.Dense, b *mat.Dense) (*mat.Dense, error) {
	rA, cA := a.Dims()
	rB, cB := b.Dims()
	if cA != rB {
		return nil, status.Errorf(codes.InvalidArgument, "incompatible matrix dimensions: %d columns in A, %d rows in B", cA, rB)
	}
	result := mat.NewDense(rA, cB, nil)
	result.Mul(a, b)
	return result, nil
}

func SumMatrix(a *mat.Dense, b *mat.Dense) (*mat.Dense, error) {
	rA, cA := a.Dims()
	rB, cB := b.Dims()
	if rA != rB || cA != cB {
		return nil, status.Errorf(codes.InvalidArgument, "matrices do not have the same dimension")
	}
	result := mat.NewDense(rA, cA, nil)
	result.Add(a, b)
	return result, nil
}

func SplitMatrix(gradientMatrix *mat.Dense, numChunks int) []*mat.Dense {
	rows, cols := gradientMatrix.Dims()
	// Assuming we're splitting by rows, calculate the number of rows per chunk
	rowsPerChunk := rows / numChunks

	var chunks []*mat.Dense
	for i := 0; i < numChunks; i++ {
		// define the row range for each chunk
		startRow := i * rowsPerChunk
		endRow := (i + 1) * rowsPerChunk
		if i == numChunks-1 {
			// the last chunk includes any remaining rows
			endRow = rows
		}
		subMatrix := gradientMatrix.Slice(startRow, endRow, 0, cols).(*mat.Dense)
		chunks = append(chunks, subMatrix)
	}
	return chunks
}

func MergeMatrix(gradients []*mat.Dense) (*mat.Dense, error) {
	// Ensure there is at least one gradient matrix
	if len(gradients) == 0 {
		return nil, errors.New("no gradient matrices provided")
	}

	expectedCols := gradients[0].RawMatrix().Cols
	var totalRows int
	for _, grad := range gradients {
		if grad.RawMatrix().Cols != expectedCols {
			return nil, errors.New("gradient matrices have different number of columns")
		}
		totalRows += grad.RawMatrix().Rows
	}

	merged := mat.NewDense(totalRows, expectedCols, nil)
	rowOffset := 0
	for _, grad := range gradients {
		gradData := grad.RawMatrix().Data
		for i := 0; i < grad.RawMatrix().Rows; i++ {
			merged.SetRow(rowOffset+i, gradData[i*grad.RawMatrix().Cols:(i+1)*grad.RawMatrix().Cols])
		}
		rowOffset += grad.RawMatrix().Rows
	}
	return merged, nil
}

// compute CrossEntropy(softmax(XW), Y) where Y is assumed to be already 1 hot encoded
func LinearCrossEntropyGradients(X *mat.Dense, W *mat.Dense, Y *mat.Dense) (*mat.Dense, float64, error) {
	rX, cX := X.Dims() // m samples * n features
	rW, cW := W.Dims() // n features * c outputs
	rY, cY := Y.Dims() // m samples * c outputs

	if cX != rW || rX != rY || cW != cY {
		return nil, 0, status.Errorf(codes.InvalidArgument, "invalid matrix dimensions when forward pass")
	}

	m := rX
	n := rW
	c := cY

	// Z = X * W
	Z := mat.NewDense(m, c, nil)
	Z.Mul(X, W)

	// Yhat = softmax(Y)
	YHat := mat.NewDense(m, c, nil)
	for i := 0; i < m; i++ {
		row := mat.Row(nil, i, Z)
		expSum := 0.0
		for j := range row {
			row[j] = math.Exp(row[j])
			expSum += row[j]
		}
		for j := range row {
			row[j] /= expSum
		}
		YHat.SetRow(i, row)
	}

	// Cross Entropy loss
	loss := 0.0
	for i := 0; i < m; i++ {
		for j := 0; j < c; j++ {
			y := Y.At(i, j)
			if y > 0 {
				loss -= y * math.Log(YHat.At(i, j))
			}
		}
	}
	loss /= float64(m)

	// Compute Gradients
	// delta = YHat - Y
	delta := mat.NewDense(m, c, nil)
	delta.Sub(YHat, Y)

	// gradW = X^T * delta / #samples=m
	gradW := mat.NewDense(n, c, nil)
	gradW.Mul(X.T(), delta)
	gradW.Scale(1/float64(m), gradW)

	return gradW, loss, nil
}

func ComputeNewWeights(W *mat.Dense, Grad *mat.Dense, learningRate float64) (*mat.Dense, error) {
	rW, cW := W.Dims()
	rG, cG := Grad.Dims()

	if rW != rG || cW != cG {
		return nil, status.Errorf(codes.InvalidArgument, "invalid matrix dimensions when updating weights")
	}

	// W' = W - learningRate * Grad
	newW := mat.NewDense(W.RawMatrix().Rows, W.RawMatrix().Cols, nil)
	for i := 0; i < W.RawMatrix().Rows; i++ {
		for j := 0; j < W.RawMatrix().Cols; j++ {
			newW.Set(i, j, W.At(i, j)-learningRate*Grad.At(i, j))
		}
	}
	return newW, nil
}
