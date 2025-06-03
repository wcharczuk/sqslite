package sqslite

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_LinkedList_Push_Pop(t *testing.T) {
	q := new(LinkedList[string])

	n0 := "0"
	n1 := "1"
	n2 := "2"
	n3 := "3"

	v, ok := q.Pop()
	// Region: empty list
	{
		require.Equal(t, false, ok)
		require.Nil(t, q.head)
		require.Nil(t, q.tail)
		require.Empty(t, v)
		require.Equal(t, 0, q.len)
	}

	// Region: push 0
	{
		q.Push(n0)
		require.NotNil(t, q.head)
		require.Nil(t, q.head.Next)
		require.NotNil(t, q.tail)
		require.Nil(t, q.tail.Previous)
		require.Equal(t, q.head, q.tail)
		require.Equal(t, 1, q.len)
		require.Equal(t, n0, q.head.Value)
		require.Equal(t, n0, q.tail.Value)
	}

	// Region: push 1
	{
		q.Push(n1)
		require.NotNil(t, q.head)
		require.Nil(t, q.head.Previous)
		require.NotNil(t, q.head.Next)
		require.Nil(t, q.head.Next.Next)
		require.NotNil(t, q.tail)
		require.NotNil(t, q.tail.Previous)
		require.Nil(t, q.tail.Previous.Previous)
		require.Nil(t, q.tail.Next)
		require.NotEqual(t, q.head, q.tail)
		require.Equal(t, q.head.Next, q.tail)
		require.Equal(t, q.tail.Previous, q.head)
		require.Equal(t, 2, q.len)
		require.Equal(t, n0, q.head.Value)
		require.Equal(t, n1, q.tail.Value)
	}

	// Region: push 2
	{
		q.Push(n2)
		require.Nil(t, q.head.Previous)
		require.NotNil(t, q.head)
		require.NotNil(t, q.head.Next)
		require.NotNil(t, q.head.Next.Next)
		require.Nil(t, q.head.Next.Next.Next)
		require.Equal(t, q.head.Next.Next, q.tail)
		require.NotNil(t, q.tail)
		require.NotNil(t, q.tail.Previous)
		require.NotNil(t, q.tail.Previous.Previous)
		require.Nil(t, q.tail.Previous.Previous.Previous)
		require.Nil(t, q.tail.Next)
		require.Equal(t, q.tail.Previous.Previous, q.head)
		require.NotEqual(t, q.head, q.tail)
		require.Equal(t, 3, q.len)
		require.Equal(t, n0, q.head.Value)
		require.Equal(t, n2, q.tail.Value)
	}

	// Region: push 3
	{
		q.Push(n3)
		require.Nil(t, q.head.Previous)
		require.NotNil(t, q.head)
		require.NotNil(t, q.head.Next)
		require.NotNil(t, q.head.Next.Next)
		require.NotNil(t, q.head.Next.Next.Next)
		require.Nil(t, q.head.Next.Next.Next.Next)
		require.Equal(t, q.head.Next.Next.Next, q.tail)
		require.NotNil(t, q.tail)
		require.NotNil(t, q.tail.Previous)
		require.NotNil(t, q.tail.Previous.Previous)
		require.NotNil(t, q.tail.Previous.Previous.Previous)
		require.Nil(t, q.tail.Previous.Previous.Previous.Previous)
		require.Equal(t, q.tail.Previous.Previous.Previous, q.head)
		require.Nil(t, q.tail.Next)
		require.NotEqual(t, q.head, q.tail)
		require.Equal(t, 4, q.len)
		require.Equal(t, n0, q.head.Value)
		require.Equal(t, n3, q.tail.Value)
	}

	// Region: pop 0
	{
		v, ok = q.Pop()
		require.Equal(t, true, ok)
		require.Equal(t, n0, v)
		require.NotNil(t, q.head)
		require.NotNil(t, q.head.Next)
		require.NotNil(t, q.head.Next.Next)
		require.Nil(t, q.head.Next.Next.Next)
		require.Equal(t, q.head.Next.Next, q.tail)
		require.NotNil(t, q.tail)
		require.NotEqual(t, q.head, q.tail)
		require.Equal(t, q.len, 3)
		require.Equal(t, n1, q.head.Value)
		require.Equal(t, n3, q.tail.Value)
	}

	// Region: pop 1
	{
		v, ok = q.Pop()
		require.Equal(t, true, ok)
		require.Equal(t, n1, v)
		require.NotNil(t, q.head)
		require.NotNil(t, q.head.Next)
		require.Nil(t, q.head.Next.Next)
		require.Equal(t, q.head.Next, q.tail)
		require.NotNil(t, q.tail)
		require.NotNil(t, q.tail.Previous)
		require.Nil(t, q.tail.Previous.Previous)
		require.Equal(t, q.tail.Previous, q.head)
		require.NotEqual(t, q.head, q.tail)
		require.Equal(t, 2, q.len)
		require.Equal(t, n2, q.head.Value)
		require.Equal(t, n3, q.tail.Value)
	}

	// Region: pop 2
	{
		v, ok = q.Pop()
		require.Equal(t, true, ok)
		require.Equal(t, n2, v)
		require.NotNil(t, q.head)
		require.Nil(t, q.head.Previous)
		require.NotNil(t, q.tail)
		require.Equal(t, q.head, q.tail)
		require.Equal(t, 1, q.len)
		require.Equal(t, n3, q.head.Value)
		require.Equal(t, n3, q.tail.Value)
	}

	// Region: pop 3
	{
		v, ok = q.Pop()
		require.Equal(t, true, ok)
		require.Equal(t, n3, v)
		require.Nil(t, q.head)
		require.Nil(t, q.tail)
		require.Equal(t, 0, q.len)
	}

	q.Push(n0)
	q.Push(n1)
	q.Push(n1)
	require.Equal(t, 3, q.len)
}
func Test_LinkedList_Remove_tail(t *testing.T) {
	q := new(LinkedList[string])

	n0 := "0"
	n1 := "1"
	n2 := "2"
	n3 := "3"

	q.Push(n0)
	q.Push(n1)
	q.Push(n2)
	n3n := q.Push(n3)

	q.Remove(n3n)

	require.Equal(t, "2", q.tail.Value)
}

func Test_LinkedList_Each(t *testing.T) {
	l := new(LinkedList[int])
	for x := 0; x < 10; x++ {
		l.Push(x)
	}

	var values []int
	for x := range l.Each() {
		values = append(values, x)
	}
	require.Equal(t, []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, values)
}

func Test_LinkedList_EachNode(t *testing.T) {
	l := new(LinkedList[int])
	for x := 0; x < 10; x++ {
		l.Push(x)
	}

	var values []int
	for x := range l.EachNode() {
		values = append(values, x.Value)
	}
	require.Equal(t, []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, values)
}

func Test_LinkedList_Consume(t *testing.T) {
	n0 := "0"
	n1 := "1"
	n2 := "2"

	rhl := new(LinkedList[string])
	rhl.Push(n0)
	rhl.Push(n1)
	rhl.Push(n2)

	require.NotNil(t, rhl.head)
	require.NotNil(t, rhl.tail)
	require.Equal(t, 3, rhl.len)

	var consumed []string
	for v := range rhl.Consume() {
		consumed = append(consumed, v)
	}
	require.Equal(t, 3, len(consumed))
	require.Equal(t, 0, rhl.len)
	require.Nil(t, rhl.head)
	require.Nil(t, rhl.tail)
	require.Equal(t, []string{"0", "1", "2"}, consumed)
}

func Test_LinkedList_ConsumeNode(t *testing.T) {
	n0 := "0"
	n1 := "1"
	n2 := "2"

	rhl := new(LinkedList[string])
	rhl.Push(n0)
	rhl.Push(n1)
	rhl.Push(n2)

	require.NotNil(t, rhl.head)
	require.NotNil(t, rhl.tail)
	require.Equal(t, 3, rhl.len)

	var consumed []string
	for v := range rhl.ConsumeNode() {
		consumed = append(consumed, v.Value)
	}
	require.Equal(t, 3, len(consumed))
	require.Equal(t, 0, rhl.len)
	require.Nil(t, rhl.head)
	require.Nil(t, rhl.tail)
	require.Equal(t, []string{"0", "1", "2"}, consumed)
}
