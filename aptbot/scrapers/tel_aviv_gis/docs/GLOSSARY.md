# Tel Aviv GIS construction

Vocabulary for construction-related records in the Tel Aviv-Yafo municipal GIS, used to tell
whether an apartment listing is near, or inside, a place where construction is happening or coming.

## Language

### Core

**Construction Feature**:
Anything one GIS layer records that signals construction at a place, from a Town Plan to active work.
_Avoid_: site (too narrow), record, item

**Kind**:
What sort of signal a Construction Feature is: building works, infrastructure works, a Town Plan,
a hazard (Dangerous-Building Order) or a Policy Zone.
_Avoid_: type (clashes with the GIS's own permit-type codes), category

**Stage**:
How far the thing has progressed: a plan is proposed, approved or cancelled; works are requested,
permitted, lapsed, active or completed; a hazard is ordered.
_Avoid_: status (clashes with the GIS's own status fields)

**Severity**:
How disruptive the thing is likely to be to residents (high, medium, low), regardless of its Stage.
_Avoid_: impact, priority

**Tag**:
A cross-layer fact about a Construction Feature that callers filter on, such as TAMA 38 rebuild or Urban Renewal.
_Avoid_: label, flag

### Works

**Permit Request**:
An application for a building permit that has been filed but not yet decided.
_Avoid_: application, pending permit

**Permit**:
A building permit that has been issued and whose work is not yet completed.
_Avoid_: license, approval

**Lapsed**:
A Permit whose validity expired before work started; the work may never happen or may be renewed.
_Avoid_: expired (an active site's permit can be past its expiry and still be fine)

**Permit Renewal**:
A request to extend an existing Permit (חידוש היתר); it says nothing by itself about the size of the work.
_Avoid_: extension

**Infrastructure Works**:
Street-level public works (light rail, roads, water, sewage, electricity) rather than work
on a private building.
_Avoid_: roadworks (too narrow), public works

### Long horizon

**Town Plan**:
A statutory plan (תב"ע) that sets what may be built in an area; it enables construction years
before any Permit Request.
_Avoid_: plan (alone, ambiguous), zoning, תכנית

**Proposed**:
The Stage of a Town Plan before approval: preparation, deposit, objections, or interim building restrictions.
_Avoid_: pending, in planning

**Approved**:
The Stage of a Town Plan or Policy Zone that is in force.
_Avoid_: valid, active (active is reserved for works under way)

**Urban Renewal**:
Redevelopment of existing housing (התחדשות עירונית), including pinui-binui and TAMA 38 rebuilds.
_Avoid_: regeneration, renovation

**Dangerous-Building Order**:
A municipal order or notice that a building is unsafe, ranging from an engineer inspection to
immediate demolition or evacuation.
_Avoid_: violation, condemnation

**Policy Zone**:
An area with published rules for future construction (e.g. which TAMA 38 rights apply), not tied
to a specific project.
_Avoid_: district, compound
