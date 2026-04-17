---
name: ui-clean-minimal
description: Use when refining operator-facing UI so screens stay clean, minimal, and action-first while optional explanation moves into contextual help.
---

# UI Clean Minimal

Use this skill for operator-console UI work when a screen, card, form, table, or settings surface feels text-heavy or visually noisy.

## Default Rules

1. Keep the interface direct.
   - Prefer labels, controls, and status.
   - Remove persistent explanatory paragraphs when the user can already infer the next step from the control layout.

2. Move optional explanation behind contextual help.
   - Use a small `?` affordance next to the relevant heading, label, or field.
   - Show the extra detail on hover and keyboard focus.
   - Keep tooltip copy short, specific, and tied to the nearby control.

3. Keep only essential inline text.
   - Validation errors
   - destructive warnings
   - required blocking status
   - live progress that changes operator decisions

4. Shorten empty states.
   - Prefer concise status such as `No provider connections yet.`
   - Avoid instructional sentences unless the empty state would otherwise be ambiguous.

5. Preserve action clarity.
   - If helper text is removed, the remaining labels and buttons must still make the next action obvious.
   - When a workflow has two similar actions, use the tooltip to explain the difference instead of a permanent paragraph.

## Good Patterns

- Section heading + `?` tooltip
- Field label + `?` tooltip
- Compact empty state + primary action button
- Brief status badge or inline status line

## Avoid

- long descriptive paragraphs under every card heading
- repeated helper copy across multiple cards
- multi-line per-field instruction blocks that remain visible after the user understands the screen
- tooltip content that duplicates the visible label without adding decision-making value
