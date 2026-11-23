# Tel Aviv GIS construction

Vocabulary for construction-related records in the Tel Aviv-Yafo municipal GIS, used to tell
whether an apartment listing is near noisy or disruptive work.

## Language

**Construction Feature**:
One piece of construction-related work at a place, as one GIS layer records it.
_Avoid_: site (too narrow), record, item

**Kind**:
What sort of work a Construction Feature is: building works or street/infrastructure works.
_Avoid_: type (clashes with the GIS's own permit-type codes), category

**Stage**:
How far the work has progressed legally or physically: requested, permitted, lapsed, active,
or completed.
_Avoid_: status (clashes with the GIS's own status fields)

**Lapsed**:
A Permit whose validity expired before work started; the work may never happen or may be renewed.
_Avoid_: expired (an active site's permit can be past its expiry and still be fine)

**Permit Renewal**:
A request to extend an existing Permit (חידוש היתר); it says nothing by itself about the size of the work.
_Avoid_: extension

**Severity**:
How disruptive the work is likely to be to nearby residents (high, medium, low).
_Avoid_: impact, priority

**Permit Request**:
An application for a building permit that has been filed but not yet decided.
_Avoid_: application, pending permit

**Permit**:
A building permit that has been issued and whose work is not yet completed.
_Avoid_: license, approval

**Infrastructure Works**:
Street-level public works (light rail, roads, water, sewage, electricity) rather than work
on a private building.
_Avoid_: roadworks (too narrow), public works
