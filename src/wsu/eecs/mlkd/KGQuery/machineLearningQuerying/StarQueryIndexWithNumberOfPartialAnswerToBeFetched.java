package wsu.eecs.mlkd.KGQuery.machineLearningQuerying;

public class StarQueryIndexWithNumberOfPartialAnswerToBeFetched {
	int starQueryIndex;
	int numberOfPartialAnswersShouldBeFetched;

	public StarQueryIndexWithNumberOfPartialAnswerToBeFetched(int starQueryIndex,
			int numberOfPartialAnswersShouldBeFetched) {
		this.starQueryIndex = starQueryIndex;
		this.numberOfPartialAnswersShouldBeFetched = numberOfPartialAnswersShouldBeFetched;
	}

	@Override
	public StarQueryIndexWithNumberOfPartialAnswerToBeFetched clone() {
		StarQueryIndexWithNumberOfPartialAnswerToBeFetched newObject = new StarQueryIndexWithNumberOfPartialAnswerToBeFetched(
				this.starQueryIndex, this.numberOfPartialAnswersShouldBeFetched);
		return newObject;
	}
}
