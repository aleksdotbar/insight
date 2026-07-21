import { afterEach, describe, expect, it, vi } from "vitest";

import { downloadBlob } from "@/components/widgets/metric-views/metric-timeseries-export";

describe("downloadBlob", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
    vi.restoreAllMocks();
  });

  it("downloads and revokes an object URL", () => {
    const createObjectURL = vi.fn(() => "blob:timeseries");
    const revokeObjectURL = vi.fn();
    const click = vi
      .spyOn(HTMLAnchorElement.prototype, "click")
      .mockImplementation(() => undefined);
    vi.stubGlobal("URL", { createObjectURL, revokeObjectURL });
    vi.stubGlobal("requestAnimationFrame", (callback: FrameRequestCallback) => {
      callback(0);
      return 1;
    });

    downloadBlob(new Blob(["data"]), "output.csv");

    expect(createObjectURL).toHaveBeenCalledOnce();
    expect(click).toHaveBeenCalledOnce();
    expect(revokeObjectURL).toHaveBeenCalledWith("blob:timeseries");
    expect(document.querySelector('a[download="output.csv"]')).toBeNull();
  });
});
