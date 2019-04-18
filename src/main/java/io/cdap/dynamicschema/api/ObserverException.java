package io.cdap.dynamicschema.api;

/**
 * Thrown when there is a visiting exception.
 */
public class ObserverException extends Exception {
  public ObserverException(String message) {
    super(message);
  }
}
