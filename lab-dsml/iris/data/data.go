package data

import (
	"encoding/csv"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"

	"gonum.org/v1/gonum/mat"
)

func oneHotEncode(labels []string) (*mat.Dense, error) {
	// Get unique labels
	labelMap := make(map[string]int)
	for _, label := range labels {
		if _, exists := labelMap[label]; !exists {
			labelMap[label] = len(labelMap)
		}
	}

	// Create a one-hot encoded matrix
	numLabels := len(labels)
	numClasses := len(labelMap)
	encoded := mat.NewDense(numLabels, numClasses, nil)

	for i, label := range labels {
		classIndex := labelMap[label]
		encoded.Set(i, classIndex, 1.0)
	}

	return encoded, nil
}

// LoadIrisDataset reads the Iris dataset, ignoring headers, and separates it into training data and targets.
func LoadIrisDataset(filePath string, testRatio float64) (*mat.Dense, *mat.Dense, *mat.Dense, *mat.Dense, error) {
	rng := rand.New(rand.NewSource(42))

	file, err := os.Open(filePath)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("error opening file: %w", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("error reading CSV: %w", err)
	}

	// Ignore the header
	records = records[1:]

	// Separate features and targets
	var features [][]float64
	var targets []string

	for _, record := range records {
		if len(record) < 5 {
			return nil, nil, nil, nil, fmt.Errorf("invalid row: %v", record)
		}

		// Parse features
		var row []float64
		for i := 0; i < 4; i++ { // First 4 columns are features
			value, err := strconv.ParseFloat(record[i], 64)
			if err != nil {
				return nil, nil, nil, nil, fmt.Errorf("error parsing feature: %w", err)
			}
			row = append(row, value)
		}
		features = append(features, row)

		// Parse target (last column)
		targets = append(targets, strings.TrimSpace(record[4]))
	}

	numRows := len(features)
	numCols := len(features[0]) + 1 // +1 for the bias term

	augmentedFeatureData := make([][]float64, numRows)
	for i, row := range features {
		augmentedRow := make([]float64, numCols)
		copy(augmentedRow[:numCols-1], row) // Copy the original row (ignoring the bias column)
		augmentedRow[numCols-1] = 1.0       // Add the bias term as the last element
		augmentedFeatureData[i] = augmentedRow
	}
	// One-hot encode targets
	targetMatrix, err := oneHotEncode(targets)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("error encoding targets: %w", err)
	}

	indices := rng.Perm(numRows)
	shuffledFeatures := make([][]float64, numRows)
	shuffledTargets := make([][]float64, numRows)
	for i, idx := range indices {
		shuffledFeatures[i] = augmentedFeatureData[idx]
		shuffledTargets[i] = targetMatrix.RawRowView(idx)
	}
	splitIndex := int(float64(numRows) * (1 - testRatio))

	// Split the data into training and testing sets
	trainFeatures := mat.NewDense(splitIndex, numCols, nil)
	trainTargets := mat.NewDense(splitIndex, len(targetMatrix.RawRowView(0)), nil)
	for i := 0; i < splitIndex; i++ {
		trainFeatures.SetRow(i, shuffledFeatures[i])
		trainTargets.SetRow(i, shuffledTargets[i])
	}
	testFeatures := mat.NewDense(numRows-splitIndex, numCols, nil)
	testTargets := mat.NewDense(numRows-splitIndex, len(targetMatrix.RawRowView(0)), nil)
	for i := splitIndex; i < numRows; i++ {
		testFeatures.SetRow(i-splitIndex, shuffledFeatures[i])
		testTargets.SetRow(i-splitIndex, shuffledTargets[i])
	}

	return trainFeatures, trainTargets, testFeatures, testTargets, nil
}
