import { useIcPerson } from "@/queries/ic-dashboard";
import { useMetricsV2Enabled } from "@/lib/feature-flags";
import { isSalesDepartment } from "@/lib/insight/is-sales-department";

import { EngineeringDashboard } from "./engineering-dashboard";
import { EngineeringDashboardV2 } from "./engineering-dashboard-v2";
import { SalesDashboard } from "./sales-dashboard";

export interface IcDashboardScreenProps {
  personId: string;
}

export function IcDashboardScreen({ personId }: IcDashboardScreenProps) {
  const personQ = useIcPerson(personId);
  const person = personQ.data ?? null;
  const v2 = useMetricsV2Enabled();
  const sales = isSalesDepartment(person?.department);
  if (sales) {
    return <SalesDashboard personId={personId} person={person} />;
  }
  if (v2) {
    return <EngineeringDashboardV2 personId={personId} person={person} />;
  }
  return <EngineeringDashboard personId={personId} person={person} />;
}
