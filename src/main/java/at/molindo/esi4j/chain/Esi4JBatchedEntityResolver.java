package at.molindo.esi4j.chain;

public interface Esi4JBatchedEntityResolver extends Esi4JEntityResolver {

	public void openResolveSession();

	/**
	 * calls {@link Esi4JEntityTask#resolveEntity(Esi4JEntityResolver)} for all
	 * tasks
	 * 
	 * @param tasks
	 *            may contain <code>null</code>
	 */
	void resolveEntities(Esi4JEntityTask[] tasks);

	public void closeResolveSession();

}
