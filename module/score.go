package module

type CalculateScore func(counts Counts) uint64

func CalculateScoreSimple(counts Counts) uint64 {
	return counts.CalledCount +
		counts.AcceptedCount<<1 +
		counts.CompletedCount<<2 +
		counts.HandlingNumber<<4
}

// 设定组件的评分
func SetScore(module Module) bool {
	calculator := module.ScoreCalculator()
	if nil == calculator {
		calculator = CalculateScoreSimple
	}
	newScore := calculator(module.Counts())
	if newScore == module.Score() {
		return false
	}
	module.SetScore(newScore)
	return true
}
