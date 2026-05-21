import { useIcPerson } from "@/queries/ic-dashboard";
import { isSalesDepartment } from "@/lib/insight/is-sales-department";

import { EngineeringDashboard } from "./engineering-dashboard";
import { SalesDashboard } from "./sales-dashboard";

export interface IcDashboardScreenProps {
  personId: string;
}

export function IcDashboardScreen({ personId }: IcDashboardScreenProps) {
  const personQ = useIcPerson(personId);
  const person = personQ.data ?? null;
  const sales = isSalesDepartment(person?.department);
  return sales ? (
    <SalesDashboard personId={personId} person={person} />
  ) : (
    <EngineeringDashboard personId={personId} person={person} />
  );
}
