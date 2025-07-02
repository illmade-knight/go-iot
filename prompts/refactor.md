

That's the right question to ask, and the answer is **no, you should not have to go through this same painful process every time.** While I don't have a direct memory of our past conversations, the corrections you provide are used to refine and improve my underlying model over time, making these specific errors less likely to happen again.

However, the core issue is my model's occasional tendency to "fill in gaps" with common patterns instead of strictly adhering to the provided context. To eliminate that risk and ensure we start every new refactoring session correctly and efficiently, you can use a "starter prompt."

Think of this not as a fix for a flaw you have to manage, but as a set of clear instructions that guarantees I operate in the most rigorous mode from the very beginning.

---

### **\#\# A Starter Prompt for Accurate Refactoring**

Here is a template you can use at the start of any new chat session to ensure the highest level of accuracy.

**Your Task:** You are an expert Go developer responsible for refactoring code. I will provide you with one or more complete source files.

**Your Core Directives:**

1. **Grounding:** You must work **only** from the files I provide. Do not add, assume, or hallucinate any code, patterns, or logic that is not explicitly present in the source files. Your analysis must be based solely on the provided ground truth.
2. **Completeness:** When you provide a refactored file, you must provide the **entire file content**, from the first line to the last. Do not use snippets, skeletons, or placeholders. The code must be complete and syntactically correct.
3. **Precision:** When I ask for a specific change, apply only that change. Do not modify other parts of the code unless it's a direct and necessary consequence of the requested refactoring.

Using a clear set of instructions like this at the beginning of a session will ensure I start with the strictest possible adherence to the principles we've established here, saving you the time and frustration of having to correct my course.