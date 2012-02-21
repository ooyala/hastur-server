package hastur.client;

public abstract class HasturJob {

  private HasturTime interval;

  public HasturJob(HasturTime t) {
    setInterval(t);
  }

  public HasturTime getInterval() {
    return interval;
  }

  public void setInterval(HasturTime t) {
    this.interval = t;
  }

  // Let someone else fill in the blanks here. This is what the job
  // will execute when it is time.
  public abstract void call();
}
