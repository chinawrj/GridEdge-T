(function initializeGridEdgeFlushCoordinator(root, factory) {
  const api = factory();
  if (typeof module === "object" && module.exports) module.exports = api;
  root.GridEdgeFlushCoordinator = api;
})(typeof globalThis === "object" ? globalThis : this, function buildFlushCoordinator() {
  "use strict";

  function createFlushCoordinator(run, { onError = async () => {} } = {}) {
    let inFlight = null;
    let requested = false;

    function request() {
      requested = true;
      if (!inFlight) {
        inFlight = (async () => {
          let result = { ok: true, published: 0 };
          while (requested) {
            requested = false;
            try {
              result = await run();
            } catch (error) {
              await onError(error);
              if (!requested) throw error;
            }
          }
          return result;
        })().finally(() => { inFlight = null; });
      }
      return inFlight;
    }

    return { request };
  }

  return { createFlushCoordinator };
});
