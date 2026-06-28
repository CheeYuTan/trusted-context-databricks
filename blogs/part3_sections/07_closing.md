## Putting It Together: Choosing the Right Join Pattern

Metric Views are not only a place to define measures.

They are also a place to define the relationship model behind those measures.

That matters for trusted context because joins are easy to get wrong:

- joining to the wrong dimension,
- missing a normalized hierarchy,
- accidentally fanning out a measure,
- mixing fact grains incorrectly,
- or making every dashboard and agent rewrite the same joins.

The pattern I want is simple:

```text
Relationships belong in the semantic layer.
Consumers should query trusted fields and measures.
```

The important question is not only:

```text
Can I join these tables?
```

The more important question is:

```text
What grain should own this metric?
```

Here is the decision guide I use:

- Use **star joins** when the fact table has direct dimension keys.
- Use **snowflake joins** when dimensions are normalized into multiple levels.
- Use **many-to-one** when the joined table enriches the source row.
- Use **one-to-many** when the joined table contributes facts below the source grain.
- Use **nested one-to-many** when facts have child facts.
- Use **sibling one-to-many** when one source entity has multiple independent fact branches.
- Use a **bridge** when multiple fact tables share dimensions but no single fact should own the source grain.

## Closing Thoughts

Part 3 focused on the relationship model behind trusted metrics.

The main lesson is that joins should not be rediscovered in every dashboard, notebook, or agent-generated query. They should be modeled once, reviewed once, and reused through the Metric View.

The next advanced topic is calculation semantics: level of detail, windows, composability, and agent metadata.
