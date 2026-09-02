namespace Klexir.EventFlow.Abstractions;

/// <summary>Controls how one event is dispatched to its registered handlers.</summary>
public enum EventDispatchMode
{
    Sequential = 0,
    Parallel = 1,
}
