import type { GatewayModel } from "@posthog/shared";
import { describe, expect, it } from "vitest";
import { availableOnboardingTestModels } from "./OnboardingTestToolsDialog";

const model = (id: string, owned_by: string, allowed = true): GatewayModel => ({
  id,
  owned_by,
  context_window: 200000,
  supports_streaming: true,
  supports_vision: false,
  allowed,
});

describe("availableOnboardingTestModels", () => {
  it("keeps each supported, plan-available desktop model", () => {
    const options = availableOnboardingTestModels([
      model("claude-opus-4-8", "anthropic"),
      model("@cf/zai-org/glm-5.2", "cloudflare"),
      model("gpt-5.5", "openai"),
      model("claude-fable-5", "anthropic", false),
      model("titan-express", "bedrock"),
    ]);

    expect(options.map((option) => option.value)).toEqual([
      "claude-opus-4-8",
      "@cf/zai-org/glm-5.2",
      "gpt-5.5",
    ]);
  });
});
