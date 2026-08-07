import { MetricName } from "@/components/widgets/metric-help-tooltip";
import { formatMetricValue } from "@/lib/format";
import { metricComparisons } from "@/lib/insight/metric-comparison";
import {
  forEntity,
  type MetricCollectionConfig,
  type NormalizedMetricResult,
} from "@/lib/metrics/collection";

/**
 * Everything else the section measures, named and valued.
 *
 * A section shows a few metrics closely, and the rest of its collection would
 * otherwise be invisible — a person could not find out that the thing was
 * measured at all, let alone what it read. The list that used to carry them
 * was folded behind "supporting and on-par metrics", which sorted them by
 * their standing against a cohort: a reader looking for emails sent had to
 * know it was unremarkable this period in order to guess where it went.
 *
 * So: no ranking, no colour, no fold. Collection order, the value or an
 * honest dash, and the catalog's own words on hover. A dash here means the
 * metric exists and holds nothing for this person — which is itself the
 * answer to "is this being measured?".
 */
export function SectionMetricIndex({
  collection,
  byKey,
  entityId,
  shown,
}: {
  collection: MetricCollectionConfig;
  byKey: Map<string, NormalizedMetricResult>;
  entityId: string;
  /** Keys already given their own block above. */
  shown: ReadonlySet<string>;
}) {
  // Alphabetical, because the only thing a reader does with this list is look
  // something up. Collection order is the order the metrics were declared in
  // and means nothing on screen; ordering by value or by standing would rank
  // them, which is the judgment this section deliberately withholds — and
  // would also make a named metric impossible to find without reading all of
  // them.
  const rest = collection.metrics
    .flatMap((m) => {
      if (shown.has(m.key)) return [];
      const metric = byKey.get(m.key);
      return metric ? [metric] : [];
    })
    .sort((a, b) => a.label.localeCompare(b.label));
  if (rest.length === 0) return null;

  return (
    <section className="rounded-xl border p-4 sm:p-5">
      <h2 className="text-xs font-medium tracking-wide text-muted-foreground uppercase">
        Also measured here
      </h2>
      {/* Multi-column flow rather than a grid: columns fill top to bottom, so
          the alphabet reads down one column and on to the next like an index.
          A grid fills across the row and scatters the same list. */}
      <dl className="gap-x-8 pt-3 sm:columns-2 xl:columns-3">
        {rest.map((metric) => (
          <div
            key={metric.metric_key}
            className="flex break-inside-avoid items-baseline justify-between gap-3 border-b border-dashed py-1.5"
          >
            <dt className="min-w-0 truncate text-xs">
              <MetricName metric={metric} />
            </dt>
            <dd className="flex shrink-0 items-baseline gap-2 text-xs tabular-nums">
              <span>
                {formatMetricValue(
                  forEntity(metric, entityId).value,
                  metric.format,
                  metric.unit
                )}
              </span>
              {/* The pool's middle, so a row is readable without opening
                  anything. Uncoloured: a list of thirty numbers lit up by
                  quartile is a scoreboard, and the reader did not ask to be
                  scored on every one of them. */}
              <span className="w-24 text-right text-muted-foreground">
                {metricComparisons(metric, null, entityId).median ?? ""}
              </span>
            </dd>
          </div>
        ))}
      </dl>
    </section>
  );
}
