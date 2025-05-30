package slant

import (
	"bytes"
	"errors"
	"io"
	"unicode"
)

// PrintString prints a phrase to a given output with the default font.
func PrintString(phrase string) (string, error) {
	buf := new(bytes.Buffer)
	if err := Print(buf, phrase); err != nil {
		return "", err
	}
	return buf.String(), nil
}

// Print prints a phrase to a given output with the default font.
func Print(output io.Writer, phrase string) error {
	phraseRunes := []rune(phrase)
	var row, charRow []rune
	var trimCount, left int
	var err error
	for r := 0; r < defaultFont.Height; r++ {
		row = nil
		for index, char := range phraseRunes {
			if char < FirstASCII || char > LastASCII {
				return errors.New("figlet; invalid input")
			}
			charRow = []rune(rowsForLetter(char, defaultFont.Letters)[r])

			if index > 0 {
				trimCount = trimAmount(phraseRunes[index-1], phraseRunes[index], defaultFont.Height, defaultFont.Letters)
				row, left = trimRightSpaceMax(row, trimCount)
				if left > 0 {
					charRow, _ = trimLeftSpaceMax(charRow, left)
				}
			}
			charRow = replaceRunes(charRow, defaultFont.Hardblank, ' ')
			row = append(row, charRow...)
		}
		_, err = io.WriteString(output, string(row)+"\n")
		if err != nil {
			return err
		}
	}
	return nil
}

// trimAmount returns the number of characters to trim.
// this is typically the minimum sum of trailing whitespace in a, and leading whitespace in b.
func trimAmount(a, b rune, height int, letters [][]string) int {
	rowsA := rowsForLetter(a, letters)
	rowsB := rowsForLetter(b, letters)

	var trimCount int
	if len(rowsA) > len(rowsB) {
		trimCount = len(rowsA)
	} else {
		trimCount = len(rowsB)
	}

	for r := 0; r < height; r++ {
		rowA := []rune(rowsA[r])
		rowB := []rune(rowsB[r])

		spaceA := countTrailingSpace(rowA)
		spaceB := countLeadingSpace(rowB)

		if trimCount > (spaceA + spaceB) {
			trimCount = spaceA + spaceB
		}
	}
	return trimCount
}

func rowsForLetter(letter rune, letters [][]string) []string {
	return letters[int(letter)-ASCIIOffset]
}

func countLeadingSpace(row []rune) int {
	for index := 0; index < len(row); index++ {
		if !unicode.IsSpace(row[index]) {
			return index
		}
	}
	return len(row)
}

func countTrailingSpace(row []rune) int {
	for index := 0; index < len(row); index++ {
		if !unicode.IsSpace(row[len(row)-(index+1)]) {
			return index
		}
	}
	return 0
}

func trimRightSpaceMax(row []rune, maxCount int) ([]rune, int) {
	var count int
	for index := 0; index < len(row) && count < maxCount; index++ {
		if !unicode.IsSpace(row[len(row)-(index+1)]) {
			break
		}
		count++
	}
	return row[:len(row)-count], maxCount - count
}

func trimLeftSpaceMax(row []rune, maxCount int) ([]rune, int) {
	for index := 0; index < len(row); index++ {
		if !unicode.IsSpace(row[index]) || index == maxCount {
			return row[index:], maxCount - index
		}
	}
	return row, maxCount
}

func replaceRunes(row []rune, oldRune, newRune rune) []rune {
	for index := range row {
		if row[index] == oldRune {
			row[index] = newRune
		}
	}
	return row
}
