# Background queries on every full page load

Last verified: 2026-11-24

Fired by the shell on every full (non-SPA) load. Ignore them, but **their presence is a health signal**: a normal
logged-in shell booted.
`FBYRPTimeLimitsEnforcementQuery`, `fetchMWChatVideoAutoplaySettingQuery`, `CometSearchBootstrapKeywordsDataSourceQuery`,
`OhaiWebPublicKeyConfigQuery`, `useMWEncryptedBackupsFetchBackupIdsV2Query`, `RTWebCallBlockSettingHooksQuery`,
`usePseudoBlockedUserInterstitialF3Query`, `MAWVerifyThreadCutover_ContactCapabilities2Query`,
`FBScreenTimeLogger_syncMutation`, `CometRecordProductUsageMutationMutation`.

Each full load costs about 10 requests of this noise, while an SPA navigation costs none. This is one reason to prefer
in-app clicks over typed URLs once a session is running (see `../../navigation/routing-spa.md`).
