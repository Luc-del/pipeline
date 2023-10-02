package pipeline

import (
	"context"
	"errors"
	"github.com/stretchr/testify/assert"
	"math"
	"testing"
	"time"
)

func TestPipelineRun(t *testing.T) {
	t.Run("cancelling pipeline should stop the processing", func(t *testing.T) {
		ctx, cl := context.WithCancel(context.Background())

		expected := []int{2, 4, 8, 16, 32}
		var idx int
		rec := func(i int) error {
			assert.Equal(t, expected[idx], i)
			idx++
			return nil
		}

		failAfter := 5
		node := func(i int) (int, error) {
			failAfter--
			if failAfter < 0 {
				cl()
			}
			return int(math.Pow(2, float64(i))), nil
		}

		var generator int
		pub := func() (int, error) {
			generator++
			return generator, nil
		}

		p := NewPipeline(Config[int]{
			publisher: pub,
			nodes:     []Node[int]{node},
			receiver:  rec,
		})

		p.Run(ctx)
		assert.Equal(t, len(expected), idx)
	})

	t.Run("cancelling upstream nodes should stop further process", func(t *testing.T) {
		expected := []int{2, 4, 8, 16, 32}
		var idx int
		rec := func(i int) error {
			assert.Equal(t, expected[idx], i)
			idx++
			return nil
		}

		failAfter := 5
		node := func(i int) (int, error) {
			failAfter--
			if failAfter < 0 {
				return 0, errors.New("dummy error")
			}
			return int(math.Pow(2, float64(i))), nil
		}

		var generator int
		pub := func() (int, error) {
			generator++
			return generator, nil
		}

		p := NewPipeline(Config[int]{
			publisher: pub,
			nodes:     []Node[int]{node},
			receiver:  rec,
		})

		p.Run(context.Background())
		assert.Equal(t, len(expected), idx)
	})

	t.Run("working with only two nodes", func(t *testing.T) {
		expected := []int{1, 2, 3, 4, 5}
		var idx int
		rec := func(i int) error {
			assert.Equal(t, expected[idx], i)
			idx++
			return nil
		}

		var generator int
		pub := func() (int, error) {
			generator++
			if generator > 5 {
				// Make context timeout
				time.Sleep(time.Second)
			}
			return generator, nil
		}

		p := NewPipeline(Config[int]{
			publisher: pub,
			receiver:  rec,
		})

		ctx, _ := context.WithTimeout(context.Background(), time.Second)

		p.Run(ctx)
		assert.Equal(t, len(expected), idx)
	})

}
