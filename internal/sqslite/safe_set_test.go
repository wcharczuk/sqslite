package sqslite

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Helper function to create a test SafeSet
func createTestSafeSet[T comparable]() *SafeSet[T] {
	return &SafeSet[T]{storage: make(map[T]struct{})}
}

func Test_SafeSet_Add_addsElement(t *testing.T) {
	set := createTestSafeSet[string]()

	set.Add("test")

	require.True(t, set.Has("test"))
}

func Test_SafeSet_Add_addsSameElementTwice(t *testing.T) {
	set := createTestSafeSet[string]()

	set.Add("test")
	set.Add("test")

	require.True(t, set.Has("test"))
	// Verify it's still just one element by counting via Each
	count := 0
	for range set.Each() {
		count++
	}
	require.Equal(t, 1, count)
}

func Test_SafeSet_Add_addsMultipleElements(t *testing.T) {
	set := createTestSafeSet[string]()

	set.Add("first")
	set.Add("second")
	set.Add("third")

	require.True(t, set.Has("first"))
	require.True(t, set.Has("second"))
	require.True(t, set.Has("third"))
}

func Test_SafeSet_Has_returnsTrueWhenElementExists(t *testing.T) {
	set := createTestSafeSet[string]()
	set.Add("test")

	ok := set.Has("test")

	require.True(t, ok)
}

func Test_SafeSet_Has_returnsFalseWhenElementNotExists(t *testing.T) {
	set := createTestSafeSet[string]()

	ok := set.Has("nonexistent")

	require.False(t, ok)
}

func Test_SafeSet_Has_returnsFalseWhenSetEmpty(t *testing.T) {
	set := createTestSafeSet[string]()

	ok := set.Has("test")

	require.False(t, ok)
}

func Test_SafeSet_Del_removesExistingElement(t *testing.T) {
	set := createTestSafeSet[string]()
	set.Add("test")

	set.Del("test")

	require.False(t, set.Has("test"))
}

func Test_SafeSet_Del_doesNothingWhenElementNotExists(t *testing.T) {
	set := createTestSafeSet[string]()
	set.Add("existing")

	set.Del("nonexistent")

	require.True(t, set.Has("existing"))
}

func Test_SafeSet_Del_doesNothingWhenSetEmpty(t *testing.T) {
	set := createTestSafeSet[string]()

	// Should not panic
	set.Del("test")

	require.False(t, set.Has("test"))
}

func Test_SafeSet_Each_yieldsAllElements(t *testing.T) {
	set := createTestSafeSet[string]()
	set.Add("first")
	set.Add("second")
	set.Add("third")

	var elements []string
	for element := range set.Each() {
		elements = append(elements, element)
	}

	require.Len(t, elements, 3)
	require.Contains(t, elements, "first")
	require.Contains(t, elements, "second")
	require.Contains(t, elements, "third")
}

func Test_SafeSet_Each_yieldsNothingWhenEmpty(t *testing.T) {
	set := createTestSafeSet[string]()

	var elements []string
	for element := range set.Each() {
		elements = append(elements, element)
	}

	require.Empty(t, elements)
}

func Test_SafeSet_Each_stopsWhenYieldReturnsFalse(t *testing.T) {
	set := createTestSafeSet[string]()
	set.Add("first")
	set.Add("second")
	set.Add("third")

	var elements []string
	for element := range set.Each() {
		elements = append(elements, element)
		if len(elements) == 2 {
			break
		}
	}

	require.Len(t, elements, 2)
}

func Test_SafeSet_Each_doesNotModifySet(t *testing.T) {
	set := createTestSafeSet[string]()
	set.Add("test")

	for range set.Each() {
		// Just iterate
	}

	require.True(t, set.Has("test"))
}

func Test_SafeSet_Consume_yieldsAllElements(t *testing.T) {
	set := createTestSafeSet[string]()
	set.Add("first")
	set.Add("second")
	set.Add("third")

	var elements []string
	for element := range set.Consume() {
		elements = append(elements, element)
	}

	require.Len(t, elements, 3)
	require.Contains(t, elements, "first")
	require.Contains(t, elements, "second")
	require.Contains(t, elements, "third")
}

func Test_SafeSet_Consume_emptiesSet(t *testing.T) {
	set := createTestSafeSet[string]()
	set.Add("test")

	for range set.Consume() {
		// Just consume
	}

	require.False(t, set.Has("test"))
}

func Test_SafeSet_Consume_yieldsNothingWhenEmpty(t *testing.T) {
	set := createTestSafeSet[string]()

	var elements []string
	for element := range set.Consume() {
		elements = append(elements, element)
	}

	require.Empty(t, elements)
}

func Test_SafeSet_Consume_stopsWhenYieldReturnsFalse(t *testing.T) {
	set := createTestSafeSet[string]()
	set.Add("first")
	set.Add("second")
	set.Add("third")

	var elements []string
	for element := range set.Consume() {
		elements = append(elements, element)
		if len(elements) == 2 {
			break
		}
	}

	// Set should NOT be cleared if we break early - this is the actual behavior
	count := 0
	for range set.Each() {
		count++
	}
	require.Equal(t, 3, count) // All elements should still be there
}

func Test_SafeSet_Consume_doesNotEmptySetWhenBreakingEarly(t *testing.T) {
	set := createTestSafeSet[string]()
	set.Add("first")
	set.Add("second")

	for element := range set.Consume() {
		if element == "first" {
			break
		}
	}

	// Set should NOT be empty when we break early - only empties after full iteration
	count := 0
	for range set.Each() {
		count++
	}
	require.Equal(t, 2, count)
}

// Test with different types
func Test_SafeSet_worksWithIntegerType(t *testing.T) {
	set := createTestSafeSet[int]()

	set.Add(42)
	set.Add(100)

	require.True(t, set.Has(42))
	require.True(t, set.Has(100))
	require.False(t, set.Has(999))

	set.Del(42)
	require.False(t, set.Has(42))
	require.True(t, set.Has(100))
}

// Test thread safety
func Test_SafeSet_concurrentOperations_maintainConsistency(t *testing.T) {
	set := createTestSafeSet[int]()
	done := make(chan bool, 3)

	// Goroutine 1: Add elements
	go func() {
		for i := range 100 {
			set.Add(i)
			time.Sleep(time.Microsecond)
		}
		done <- true
	}()

	// Goroutine 2: Check elements
	go func() {
		for range 100 {
			set.Has(50)
			time.Sleep(time.Microsecond)
		}
		done <- true
	}()

	// Goroutine 3: Delete some elements
	go func() {
		for i := range 50 {
			set.Del(i * 2) // Delete even numbers
			time.Sleep(time.Microsecond)
		}
		done <- true
	}()

	// Wait for all goroutines to complete
	for range 3 {
		<-done
	}

	// Verify some basic consistency - should have some elements
	count := 0
	for range set.Each() {
		count++
	}
	// Should have at least some odd numbers remaining
	require.True(t, count > 0)
}

func Test_SafeSet_concurrentConsumeOperations_maintainConsistency(t *testing.T) {
	set := createTestSafeSet[string]()

	// Add some initial elements
	for i := range 10 {
		set.Add(string(rune('a' + i)))
	}

	done := make(chan bool, 2)

	// Goroutine 1: Add more elements
	go func() {
		for i := range 50 {
			set.Add(string(rune('A' + i%26)))
			time.Sleep(time.Microsecond)
		}
		done <- true
	}()

	// Goroutine 2: Consume elements periodically
	go func() {
		for range 10 {
			for range set.Consume() {
				// Just consume
			}
			time.Sleep(100 * time.Microsecond)
		}
		done <- true
	}()

	// Wait for all goroutines to complete
	for range 2 {
		<-done
	}

	// After final consume, set should be empty
	for range set.Consume() {
		// Final consume
	}

	count := 0
	for range set.Each() {
		count++
	}
	require.Equal(t, 0, count)
}

func Test_SafeSet_addAndConsumeLifecycle_maintainConsistency(t *testing.T) {
	set := createTestSafeSet[string]()

	// Add elements
	set.Add("first")
	set.Add("second")
	set.Add("third")

	// Consume all
	consumed := make([]string, 0)
	for element := range set.Consume() {
		consumed = append(consumed, element)
	}

	require.Len(t, consumed, 3)

	// Set should be empty now
	count := 0
	for range set.Each() {
		count++
	}
	require.Equal(t, 0, count)

	// Should be able to add elements again
	set.Add("new")
	require.True(t, set.Has("new"))
}
