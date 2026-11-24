# Ontology rules

> **v4.4:** hub nodes were removed and every node now carries exactly one product label; contacts reach their data through collection nodes. Where this document mentions hubs or several scope labels per node, README section 5 is authoritative.


These rules are enforced by `python main.py validate`. A config that breaks one does not start.

## 1. Three layers, and only the first one defines concepts

```
effective(org) = core.yaml  +  domains/<industry>.yaml  +  org<id>.yaml
```

- **core.yaml** defines every concept: base labels, keys, relationships, state labels, and dimension nodes.
- **The industry pack** is chosen from `accounts.organizations.industry_type`, on an exact or whole-word match. It may only **add** industry-specific classification labels, each with a `when:` rule. It may also add extra values to a shared core label.
- **org<id>.yaml** holds products, custom fields and tenant-only additions. It may only add.

## 2. One simple English name per concept, in every industry

- **No synonyms.** A contact is `Contact` in a hospital, a school and a bank. `Patient`, `Learner` and `Shopper` are not labels.
  - Industry wording belongs in the UI and in values, not in the graph.
  - `validate` rejects `naming`, `role_naming`, `naming_overrides`, `extend_synonyms` and `synonyms` anywhere in a pack.
- **No unconditional industry labels.** An industry label with no `when:` is a rename of the core label, so it is rejected.
- **No reuse of a core label on another table.** For example, putting `Subscription` on `crm.contracts` would merge customer contracts with Worktual's own module licences.
- **Labels are PascalCase English words.**
- **Relationship types are UPPER_SNAKE_CASE verbs.** They are written lowercase in YAML and upper-cased on load.

## 3. Role and state are labels, and exactly the right ones at a time

Every upsert removes every state or role label the table can set, then adds back the ones that match the current row. Because of that:

- `(:Contact:Lead)` → `(:Contact:Prospect)` → `(:Contact:Customer)`: the contact never has two role labels.
- A ticket that moves `Open → Pending → Resolved` goes `OpenTicket → InProgressTicket → ResolvedTicket`. Stored `Pending` is shown as "In Progress" in the product.
- Matching ignores case (`Open` = `open`).
- A state that means the same thing everywhere is defined **once, in core**: ActiveContract, WonDeal, PaidInvoice, CompletedMeeting, ActiveProduct and so on.

## 4. Merge what is one thing, keep it fine-grained with static labels

Tables describing one real-world thing merge onto one label with `merges_into`. Each source table adds a static label with `sublabels: {static: X}`.

| One concept | Static labels by source |
|---|---|
| `Staff` | ContactCentreStaff, SupportStaff |
| `LineItem` | QuoteLine, OrderLine, InvoiceLine |
| `ConnectedChannel` | EmailChannel, ChatChannel, SocialChannel |
| `BotPolicy` | ChannelBotPolicy, EmailBotPolicy, WhatsAppBotPolicy |
| `SLA` | ConversationSLA, TicketSLA |

## 5. Dimensions are nodes, and their values carry the industry

`inventory.products.category` → `(:ProductCategory)`, `subcategory` → `(:ProductSubcategory)-[:PART_OF]->(:ProductCategory)`, and `brand` → `(:Brand)`. The same applies to `crm.companies.industry` → `(:Industry)`.

"Cardiology", "Wealth" and "Apartments" are category **values**, not seven differently named labels.

## 6. No phantom nodes; every MERGE is an index seek

- An edge may create its target only when the target is a declared stub: a hub, `Channel`, `PlatformApp`, `Website`, `LogPeriod`, `EntityType`, or a promoted dimension (`ProductCategory`, `ProductSubcategory`, `Brand`, `Industry`).
- Entity targets are MATCHed. When the entity has not arrived yet, the edge is skipped and `scripts/reconcile.py` reports it.
- Collections hang off their owner, and the owner is matched, never created: `(:Contact)-[:HAS_EVENT_HISTORY]->(:EventHistory)-[:HAS_PERIOD]->(:LogPeriod)-[:CONTAINS]->(:Event)`.
- Every label that is MERGEd has a uniqueness constraint, so its MERGE is an index seek rather than a scan that grows with the graph.

## 7. Excluded tables

| Table | Why | What happens instead |
|---|---|---|
| `campaign.branches` | not maintained by the apps | `emit_to_graph: false`; `branch_id` stays a plain property; no IN_BRANCH edges |
| `accounts.setup_channel_configs` | not maintained by the apps | `emit_to_graph: false`; the email / webchat / social config rows own `ConnectedChannel` directly and hang off `SetupHub` |

Both tables stay subscribed, so tenant resolution of their child rows still works.

## 8. Embeddings carry meaning, labels carry state

- `embed.fields` lists only descriptive text: subject, description, notes, name.
- Status, priority, stage and type are **not** embedded. They are labels, and they change often; embedding them would force a re-embed on every status change.
- The embedding text names its fields: `Ticket. subject: Login fails. description: …`.

## Label catalog (generated from core.yaml; run `python main.py dry-run` to see the Cypher)

| Label | Hub | Labels it can carry | Source tables |
|---|---|---|---|
| Assignment | ContactCentreHub |  | `ccass.conversation_assignments` |
| Attachment | ContactCentreHub |  | `ccass.conversation_message_attachments` |
| BotDeployment | ContactCentreHub |  | `ccass.nlp_skill_bindings` |
| BotPolicy | ContactCentreHub | ChannelBotPolicy, EmailBotPolicy, WhatsAppBotPolicy | `ccass.channel_automation_policies`, `ccass.email_mailbox_policies`, `ccass.whatsapp_channel_policies` |
| BotSession | ContactCentreHub |  | `ccass.nlp_conversations` |
| BotSkill | ContactCentreHub |  | `ccass.nlp_skills` |
| BotSkillVersion | ContactCentreHub |  | `ccass.nlp_skill_versions` |
| BotTool | ContactCentreHub |  | `ccass.nlp_skill_tools` |
| Channel | ContactCentreHub |  |  |
| ChannelCapacity | ContactCentreHub |  | `ccass.agent_capacity` |
| ChatSession | ContactCentreHub |  | `ccass.webchat_sessions` |
| ContactCentreHub | ContactCentreHub |  |  |
| Conversation | ContactCentreHub | BotHandled, StaffHandled, ChatConversation, EmailConversation, WhatsAppConversation, InstagramConversation, FacebookConversation, VoiceConversation | `ccass.conversations` |
| ConversationMemory | ContactCentreHub |  | `ccass.conversation_memory` |
| ConversationOutcome | ContactCentreHub |  | `ccass.conversation_dispositions` |
| InternalNote | ContactCentreHub | ConversationNote, WorkspaceNote | `ccass.conversation_notes`, `ccass.workspace_common_notes` |
| MissedConversation | ContactCentreHub |  | `ccass.agent_missed_contacts` |
| OutcomeCode | ContactCentreHub |  | `ccass.dispositions` |
| Participant | ContactCentreHub |  | `ccass.conversation_participants` |
| Queue | ContactCentreHub |  | `ccass.queues`, `ticketing.agent_queues` |
| RoutingAttempt | ContactCentreHub |  | `ccass.routing_attempts` |
| RoutingHistory | ContactCentreHub |  |  |
| RoutingPolicy | ContactCentreHub |  | `ccass.routing_policies` |
| SLA | ContactCentreHub | ConversationSLA, TicketSLA | `ccass.sla_policies`, `ticketing.sla_policies` |
| Skill | ContactCentreHub |  | `ccass.skills` |
| SlaTimer | ContactCentreHub |  | `ccass.conversation_sla_state` |
| Staff | ContactCentreHub | ContactCentreStaff, SupportStaff | `ccass.agents`, `ticketing.agents` |
| StaffAvailability | ContactCentreHub |  | `ccass.agent_presence` |
| StatusReason | ContactCentreHub |  | `ccass.presence_reason_codes` |
| Task | ContactCentreHub |  | `ccass.agent_tasks` |
| Transfer | ContactCentreHub |  | `ccass.conversation_transfers` |
| ActivationTask | CustomerDataHub |  | `cdp.activation_tasks` |
| Alert | CustomerDataHub |  | `cdp.anomaly_alerts` |
| AlertRule | CustomerDataHub |  | `cdp.anomaly_rules` |
| AlertSet | CustomerDataHub |  |  |
| AlertThreshold | CustomerDataHub |  | `cdp.anomaly_threshold_state` |
| Contact | CustomerDataHub | Lead, Prospect, Customer | `cdp.customer_profile` |
| ContactFact | CustomerDataHub |  | `cdp.semantic_memory` |
| ContactIdentity | CustomerDataHub |  | `cdp.contact_identities`, `cdp.identity_keys` |
| ContactInsight | CustomerDataHub |  | `cdp.ai_insights` |
| ContactScore | CustomerDataHub |  | `cdp.customer_scores` |
| ContactValue | CustomerDataHub |  | `cdp.gold_customer_metrics` |
| ConversationHistory | CustomerDataHub |  |  |
| ConversationSummary | CustomerDataHub |  | `cdp.interactions` |
| CustomerDataHub | CustomerDataHub |  |  |
| DailyMetric | CustomerDataHub |  | `cdp.customer_metrics` |
| DataSource | CustomerDataHub |  | `cdp.source_systems` |
| DraftAudience | CustomerDataHub |  |  |
| EngagementRule | CustomerDataHub |  | `cdp.procedural_memory` |
| Event | CustomerDataHub | OnlineEvent, PageViewEvent, FormEvent, InteractionEvent, ConversionEvent | `cdp.customer_events` |
| EventHistory | CustomerDataHub |  |  |
| EventMemory | CustomerDataHub |  | `cdp.episodic_memory` |
| InsightSet | CustomerDataHub |  |  |
| JourneyHistory | CustomerDataHub |  |  |
| JourneyStatus | CustomerDataHub |  | `cdp.journey_states` |
| JourneyStep | CustomerDataHub |  | `cdp.customer_journey` |
| KeyMetric | CustomerDataHub |  | `cdp.gold_campaign_lookup` |
| KeyMetricSet | CustomerDataHub |  |  |
| MemoryStore | CustomerDataHub |  |  |
| MessageHistory | CustomerDataHub |  |  |
| MetricHistory | CustomerDataHub |  |  |
| NextBestAction | CustomerDataHub |  | `cdp.customer_recommendations` |
| NextBestActionSet | CustomerDataHub |  |  |
| Persona | CustomerDataHub |  | `cdp.persona_definitions` |
| PersonaEvidence | CustomerDataHub |  | `cdp.persona_snapshots` |
| PredictionModel | CustomerDataHub |  | `cdp.agentic_model` |
| Purchase | CustomerDataHub | PaidPurchase, RefundedPurchase, CancelledPurchase, FulfilledPurchase, OpenPurchase | `cdp.orders` |
| Segment | CustomerDataHub |  | `cdp.segments` |
| Session | CustomerDataHub | OnlineSession, CampaignSession, BouncedSession | `cdp.customer_sessions` |
| SessionHistory | CustomerDataHub |  |  |
| Touchpoint | CustomerDataHub |  | `cdp.customer_interactions` |
| TouchpointHistory | CustomerDataHub |  |  |
| BlockedEmail | MarketingHub |  | `campaign.email_suppressions` |
| BounceDomain | MarketingHub |  | `campaign.email_return_paths` |
| Campaign | MarketingHub | LiveCampaign, ScheduledCampaign | `campaign.campaigns` |
| CampaignDraft | MarketingHub |  | `campaign.campaign_agent_sessions` |
| CampaignIdea | MarketingHub |  | `campaign.campaign_recommendations` |
| ChannelPlan | MarketingHub |  | `campaign.campaign_channels` |
| ChannelReadiness | MarketingHub |  | `campaign.channel_sending_profiles` |
| ChannelSettings | MarketingHub | PushSettings, SmsSettings | `campaign.push_channel_settings`, `campaign.sms_channel_settings` |
| ContentBatch | MarketingHub |  |  |
| EmailDomain | MarketingHub | VerifiedDomain | `campaign.email_domains` |
| EmailEngagement | MarketingHub | Unsubscribed | `campaign.campaign_contact_engagement` |
| IndustryPack | MarketingHub |  | `campaign.tenant_industry_packs` |
| MarketingHub | MarketingHub |  |  |
| Message | MarketingHub | OpenedMessage, ClickedMessage, FailedMessage | `campaign.campaign_send_recipients` |
| MessageTemplate | MarketingHub |  | `campaign.campaign_channel_templates` |
| Notification | MarketingHub | MarketingNotification, SalesNotification, SupportNotification | `campaign.app_notifications`, `crm.notifications`, `ticketing.notifications` |
| PushSubscription | MarketingHub |  | `campaign.push_endpoints` |
| RegisteredTemplate | MarketingHub | PendingTemplate, RejectedTemplate | `campaign.regulated_channel_templates` |
| SendBatch | MarketingHub |  | `campaign.campaign_send_batches` |
| SenderEmail | MarketingHub |  | `campaign.email_senders` |
| SenderProfile | MarketingHub |  | `campaign.email_org_profiles` |
| SendingLimit | MarketingHub |  | `campaign.email_sending_caps` |
| Subscription | MarketingHub |  | `campaign.organization_products` |
| TemplateVersion | MarketingHub |  | `campaign.campaign_template_messages` |
| Visitor | MarketingHub | AnonymousVisitor, KnownVisitor | `campaign.website_contacts` |
| AddOn | ProductCatalog |  | `crm.product_addons` |
| Brand | ProductCatalog |  |  |
| PriceList | ProductCatalog |  | `crm.price_books` |
| PriceListEntry | ProductCatalog |  | `crm.price_book_entries` |
| PricingTier | ProductCatalog |  | `crm.product_addon_tiers` |
| Product | ProductCatalog | ActiveProduct, ArchivedProduct, RecurringProduct, OneTimeProduct, ConfigurableProduct, StockedProduct, SeatBasedProduct, AgentBasedProduct, UsageBasedProduct, FlatRateProduct | `inventory.products` |
| ProductCatalog | ProductCatalog |  |  |
| ProductCategory | ProductCatalog |  |  |
| ProductPlan | ProductCatalog | ActivePlan, RecurringPlan, MonthlyPlan, AnnualPlan, MeteredPlan | `crm.product_offers` |
| ProductSubcategory | ProductCatalog |  |  |
| Account | SalesHub | Partner, SubsidiaryAccount, ExpansionCandidate | `crm.companies` |
| AccountLocation | SalesHub |  | `crm.company_locations` |
| AiMemory | SalesHub |  | `crm.agent_memories` |
| ApprovalRequest | SalesHub |  | `crm.process_confirmations` |
| AssignmentPolicy | SalesHub |  | `crm.org_assignment_settings` |
| BillingSchedule | SalesHub | ActiveBillingSchedule | `crm.contract_billing_schedules` |
| CoachingNote | SalesHub |  | `crm.coaching_insights` |
| ContactActivity | SalesHub | EmailActivity, TaskActivity, MeetingActivity | `crm.contact_activities` |
| Contract | SalesHub | ActiveContract, EndedContract, AutoRenewContract, MonthlyContract, QuarterlyContract, AnnualContract | `crm.contracts` |
| Deal | SalesHub | OpenDeal, WonDeal, LostDeal, CommitDeal, BestCaseDeal | `crm.deals` |
| DealActivity | SalesHub |  | `crm.deal_activities` |
| DocumentTemplate | SalesHub | InvoiceTemplate, ProposalTemplate, QuoteTemplate | `crm.invoice_templates`, `crm.proposal_templates`, `crm.quote_templates` |
| Industry | SalesHub |  |  |
| IndustryProfile | SalesHub |  | `crm.organization_profiles` |
| Invoice | SalesHub | UnpaidInvoice, OverdueInvoice, PaidInvoice, EscalatedInvoice | `crm.invoices` |
| LeadEnrichment | SalesHub |  | `crm.lead_enrichment_snapshots` |
| LeadJob | SalesHub | EnrichmentJob, LifecycleJob, NurtureJob, ScoringJob | `crm.lead_enrichment_jobs`, `crm.lead_lifecycle_jobs`, `crm.lead_nurture_jobs`, `crm.lead_scoring_jobs` |
| LeadSummary | SalesHub |  | `crm.lead_summaries` |
| LineItem | SalesHub | InvoiceLine, OrderLine, QuoteLine | `crm.invoice_lines`, `crm.order_lines`, `crm.quote_lines` |
| LiveMeeting | SalesHub |  | `crm.meeting_assist_sessions` |
| Meeting | SalesHub | ScheduledMeeting, CompletedMeeting, MissedMeeting, CancelledMeeting, OnlineMeeting | `crm.appointments` |
| MeetingOutcome | SalesHub |  | `crm.meeting_analyses` |
| PrivacyRequest | SalesHub |  | `crm.privacy_requests` |
| Proposal | SalesHub |  | `crm.proposals` |
| Quote | SalesHub |  | `crm.quotes` |
| SalesOrder | SalesHub |  | `crm.sales_orders` |
| SalesProfile | SalesHub | MarketingOptedOut, QualifiedSalesProfile | `crm.contact_profiles` |
| SalesRepCapacity | SalesHub |  | `crm.sales_user_assignment_profiles` |
| TranscriptLine | SalesHub |  | `crm.meeting_transcript_chunks` |
| ActivityHistory | SetupHub |  |  |
| BusinessHours | SetupHub |  | `accounts.setup_business_hours` |
| BusinessProfile | SetupHub |  | `accounts.setup_knowledge_org_profiles` |
| ConnectedChannel | SetupHub | EmailChannel, SocialChannel, ChatChannel | `accounts.setup_email_configs`, `accounts.setup_social_configs`, `accounts.setup_webchat_configs` |
| Employee | SetupHub |  | `accounts.setup_users` |
| Holiday | SetupHub |  | `accounts.setup_holidays` |
| HolidayCalendar | SetupHub |  | `accounts.setup_holiday_calendars` |
| Inbox | SetupHub |  |  |
| KnowledgeFile | SetupHub |  | `accounts.setup_knowledge_documents` |
| LogPeriod | SetupHub |  |  |
| PhoneNumber | SetupHub |  | `accounts.setup_voice_dids` |
| PlatformApp | SetupHub |  |  |
| QueueSetup | SetupHub |  | `accounts.setup_ccaas_queues` |
| Role | SetupHub |  | `accounts.setup_roles` |
| RoutingProfile | SetupHub | DraftRoutingProfile, LiveRoutingProfile | `accounts.setup_queue_routing_profiles`, `ccass.queue_routing_profiles` |
| SetupHub | SetupHub |  |  |
| Shift | SetupHub |  | `accounts.setup_business_hour_shifts` |
| Team | SetupHub |  | `accounts.setup_teams` |
| Tenant | SetupHub |  | `accounts.organizations` |
| VoicePromptSet | SetupHub |  | `accounts.setup_voice_prompt_profiles` |
| Approval | SupportHub |  | `ticketing.proactive_approvals` |
| AssistantSession | SupportHub |  | `ticketing.conversation_contexts` |
| AuditEvent | SupportHub |  | `ticketing.audit_events` |
| AutomationRule | SupportHub |  | `ticketing.automation_rules` |
| Company | SupportHub |  | `ticketing.ticketing_accounts` |
| InboundEmail | SupportHub |  | `ticketing.email_message_refs` |
| KnowledgeArticle | SupportHub |  | `ticketing.kb_articles` |
| Macro | SupportHub |  | `ticketing.macros` |
| Note | SupportHub | PublicReply, PrivateNote | `ticketing.ticket_notes` |
| OutboundEmail | SupportHub |  | `ticketing.outbound_email_log` |
| PortalSettings | SupportHub |  | `ticketing.org_settings` |
| ProactiveDecision | SupportHub |  | `ticketing.proactive_audit` |
| ProactiveEvent | SupportHub |  |  |
| ProactiveHistory | SupportHub |  |  |
| Recommendation | SupportHub |  | `ticketing.proactive_recommendations` |
| SupportHub | SupportHub |  |  |
| Ticket | SupportHub | OpenTicket, InProgressTicket, ReopenedTicket, OverdueTicket, ResolvedTicket, UrgentTicket, BillingQuery, Complaint, DocumentRequest, AccessRequest | `ticketing.tickets` |
| TicketHistory | SupportHub |  |  |
| WorkflowEvent | SupportHub |  | `ticketing.workflow_events` |
| WebTrackingHub | WebTrackingHub |  |  |
| Website | WebTrackingHub |  | `digital.tracked_sites` |

## Relationships (meta-graph)

| From | Relationship | To |
|---|---|---|
| Account | `HAS_LOCATION` | AccountLocation |
| Account | `IN_INDUSTRY` | Industry |
| Account | `OWNED_BY` | Employee |
| Account | `PART_OF` | Account |
| AccountLocation | `OWNED_BY` | Employee |
| ActivityHistory | `HAS_PERIOD` | LogPeriod |
| AddOn | `ADD_ON_FOR` | Product |
| AddOn | `HAS_TIER` | PricingTier |
| Alert | `TRIGGERED_BY` | AlertRule |
| AlertRule | `HAS_THRESHOLD` | AlertThreshold |
| AlertSet | `HAS_ALERT` | Alert |
| ApprovalRequest | `DECIDED_BY` | Employee |
| ApprovalRequest | `FOR_DEAL` | Deal |
| ApprovalRequest | `WAITING_ON` | Employee |
| Assignment | `ASSIGNS` | Conversation |
| Assignment | `OFFERED_TO` | Staff |
| AssistantSession | `HAS_EVENT` | WorkflowEvent |
| AssistantSession | `USED_BY` | Staff |
| AuditEvent | `PERFORMED_BY` | Staff |
| BillingSchedule | `FOR_ACCOUNT` | Account |
| BotPolicy | `DEFAULT_QUEUE` | Queue |
| BotPolicy | `FOR_CHANNEL` | Channel |
| BotSession | `USED_VERSION` | BotSkillVersion |
| BotSkill | `HAS_VERSION` | BotSkillVersion |
| BotSkill | `OWNED_BY` | Employee |
| BotSkillVersion | `DEPLOYED_AS` | BotDeployment |
| BotSkillVersion | `HAS_TOOL` | BotTool |
| BusinessHours | `FOLLOWS_HOLIDAYS` | HolidayCalendar |
| BusinessHours | `HAS_SHIFT` | Shift |
| Campaign | `FROM_DRAFT` | CampaignDraft |
| Campaign | `HAS_BATCH` | SendBatch |
| Campaign | `HAS_CHANNEL_PLAN` | ChannelPlan |
| CampaignDraft | `CREATED_BY` | Employee |
| CampaignDraft | `HAS_CONTENT_BATCH` | ContentBatch |
| ChannelPlan | `USES_TEMPLATE` | MessageTemplate |
| ChannelReadiness | `FOR_CHANNEL` | Channel |
| CoachingNote | `ABOUT_EMPLOYEE` | Employee |
| CoachingNote | `ANALYSIS_OF` | ContactActivity |
| ConnectedChannel | `ROUTES_TO` | QueueSetup |
| Contact | `HAS_ACTIVITY_HISTORY` | ActivityHistory |
| Contact | `HAS_CONVERSATION_HISTORY` | ConversationHistory |
| Contact | `HAS_EMAIL_ENGAGEMENT` | EmailEngagement |
| Contact | `HAS_EVENT_HISTORY` | EventHistory |
| Contact | `HAS_INSIGHT_SET` | InsightSet |
| Contact | `HAS_JOURNEY_HISTORY` | JourneyHistory |
| Contact | `HAS_JOURNEY_STATUS` | JourneyStatus |
| Contact | `HAS_MEMORY_STORE` | MemoryStore |
| Contact | `HAS_MESSAGE_HISTORY` | MessageHistory |
| Contact | `HAS_METRIC_HISTORY` | MetricHistory |
| Contact | `HAS_NEXT_BEST_ACTION_SET` | NextBestActionSet |
| Contact | `HAS_PUSH_SUBSCRIPTION` | PushSubscription |
| Contact | `HAS_SCORE` | ContactScore |
| Contact | `HAS_SESSION_HISTORY` | SessionHistory |
| Contact | `HAS_TOUCHPOINT_HISTORY` | TouchpointHistory |
| Contact | `HAS_VALUE` | ContactValue |
| Contact | `IDENTIFIED_BY` | ContactIdentity |
| Contact | `IN_SEGMENT` | Segment |
| Contact | `MATCHES_PERSONA` | Persona |
| Contact | `SAME_PERSON_AS` | Contact |
| Contact | `TARGETED_BY` | Campaign |
| ContactActivity | `ASSIGNED_TO` | Employee |
| ContactActivity | `SCHEDULED_AS` | Meeting |
| ContactCentreHub | `HAS_BOT_POLICY` | BotPolicy |
| ContactCentreHub | `HAS_BOT_SKILL` | BotSkill |
| ContactCentreHub | `HAS_NOTE` | InternalNote |
| ContactCentreHub | `HAS_OUTCOME_CODE` | OutcomeCode |
| ContactCentreHub | `HAS_QUEUE` | Queue |
| ContactCentreHub | `HAS_SKILL` | Skill |
| ContactCentreHub | `HAS_SLA` | SLA |
| ContactCentreHub | `HAS_STATUS_REASON` | StatusReason |
| ContentBatch | `CONTAINS` | MessageTemplate |
| Contract | `FOR_ACCOUNT` | Account |
| Contract | `FROM_DEAL` | Deal |
| Contract | `HAS_BILLING_SCHEDULE` | BillingSchedule |
| Contract | `RENEWED_BY` | Deal |
| Conversation | `CONTINUES` | Conversation |
| Conversation | `HANDLED_BY` | Staff |
| Conversation | `HAS_ATTACHMENT` | Attachment |
| Conversation | `HAS_BOT_SESSION` | BotSession |
| Conversation | `HAS_CHAT_SESSION` | ChatSession |
| Conversation | `HAS_MEMORY` | ConversationMemory |
| Conversation | `HAS_NOTE` | InternalNote |
| Conversation | `HAS_OUTCOME` | ConversationOutcome |
| Conversation | `HAS_PARTICIPANT` | Participant |
| Conversation | `HAS_SLA_TIMER` | SlaTimer |
| Conversation | `ROUTED_TO` | Queue |
| ConversationHistory | `HAS_PERIOD` | LogPeriod |
| ConversationOutcome | `APPLIED_BY` | Employee |
| ConversationSummary | `ROUTED_TO` | Queue |
| ConversationSummary | `SUMMARY_OF` | Conversation |
| CustomerDataHub | `HAS_DATA_SOURCE` | DataSource |
| CustomerDataHub | `HAS_ENGAGEMENT_RULE` | EngagementRule |
| CustomerDataHub | `HAS_PERSONA` | Persona |
| CustomerDataHub | `HAS_PREDICTION_MODEL` | PredictionModel |
| CustomerDataHub | `HAS_SEGMENT` | Segment |
| Deal | `AT_LOCATION` | AccountLocation |
| Deal | `HAS_ACTIVITY` | DealActivity |
| Deal | `OWNED_BY` | Employee |
| Deal | `PRIMARY_CONTACT` | Contact |
| Deal | `WITH_ACCOUNT` | Account |
| DealActivity | `CREATED_BY` | Employee |
| DraftAudience | `CONTAINS` | Contact |
| Employee | `FOLLOWS_HOURS` | BusinessHours |
| Employee | `HAS_SALES_CAPACITY` | SalesRepCapacity |
| Employee | `HOLDS_ROLE` | Role |
| Employee | `MEMBER_OF` | Team |
| Event | `OCCURS_IN` | Session |
| Event | `ON_WEBSITE` | Website |
| EventHistory | `HAS_PERIOD` | LogPeriod |
| HolidayCalendar | `HAS_HOLIDAY` | Holiday |
| InboundEmail | `ABOUT_TICKET` | Ticket |
| Inbox | `HAS_PERIOD` | LogPeriod |
| InsightSet | `CONTAINS` | ContactInsight |
| InsightSet | `CONTAINS` | LeadEnrichment |
| InsightSet | `CONTAINS` | LeadSummary |
| InsightSet | `CONTAINS` | PersonaEvidence |
| InternalNote | `WRITTEN_BY` | Employee |
| Invoice | `APPROVED_BY` | Employee |
| Invoice | `FOR_ACCOUNT` | Account |
| Invoice | `FOR_ORDER` | SalesOrder |
| Invoice | `HAS_LINE` | LineItem |
| Invoice | `ON_BILLING_SCHEDULE` | BillingSchedule |
| Invoice | `UNDER_CONTRACT` | Contract |
| JourneyHistory | `HAS_PERIOD` | LogPeriod |
| JourneyStep | `OCCURS_IN` | Session |
| KeyMetricSet | `HAS_METRIC` | KeyMetric |
| KnowledgeArticle | `WRITTEN_BY` | Staff |
| KnowledgeFile | `UPLOADED_BY` | Employee |
| LeadJob | `ABOUT_CUSTOMER` | Contact |
| LeadJob | `FOR_ACCOUNT` | Account |
| LeadJob | `FOR_DEAL` | Deal |
| LineItem | `FOR_ADD_ON` | AddOn |
| LineItem | `FOR_PRODUCT` | Product |
| LiveMeeting | `ASSISTS` | ContactActivity |
| LiveMeeting | `HAS_TRANSCRIPT_LINE` | TranscriptLine |
| LiveMeeting | `HOSTED_BY` | Employee |
| LogPeriod | `CONTAINS` | ActivationTask |
| LogPeriod | `CONTAINS` | Assignment |
| LogPeriod | `CONTAINS` | ContactActivity |
| LogPeriod | `CONTAINS` | Conversation |
| LogPeriod | `CONTAINS` | ConversationSummary |
| LogPeriod | `CONTAINS` | DailyMetric |
| LogPeriod | `CONTAINS` | Event |
| LogPeriod | `CONTAINS` | JourneyStep |
| LogPeriod | `CONTAINS` | Message |
| LogPeriod | `CONTAINS` | MissedConversation |
| LogPeriod | `CONTAINS` | Notification |
| LogPeriod | `CONTAINS` | Purchase |
| LogPeriod | `CONTAINS` | RoutingAttempt |
| LogPeriod | `CONTAINS` | Session |
| LogPeriod | `CONTAINS` | Task |
| LogPeriod | `CONTAINS` | Touchpoint |
| LogPeriod | `CONTAINS` | Transfer |
| MarketingHub | `HAS_BOUNCE_DOMAIN` | BounceDomain |
| MarketingHub | `HAS_CAMPAIGN` | Campaign |
| MarketingHub | `HAS_CAMPAIGN_IDEA` | CampaignIdea |
| MarketingHub | `HAS_CHANNEL_READINESS` | ChannelReadiness |
| MarketingHub | `HAS_CHANNEL_SETTINGS` | ChannelSettings |
| MarketingHub | `HAS_EMAIL_DOMAIN` | EmailDomain |
| MarketingHub | `HAS_REGISTERED_TEMPLATE` | RegisteredTemplate |
| MarketingHub | `HAS_SENDER_EMAIL` | SenderEmail |
| MarketingHub | `HAS_SENDER_PROFILE` | SenderProfile |
| MarketingHub | `HAS_SENDING_LIMIT` | SendingLimit |
| Meeting | `HOSTED_BY` | Employee |
| MeetingOutcome | `ANALYSIS_OF` | ContactActivity |
| MeetingOutcome | `FOR_DEAL` | Deal |
| MemoryStore | `CONTAINS` | ContactFact |
| MemoryStore | `CONTAINS` | EventMemory |
| Message | `IN_BATCH` | SendBatch |
| MessageHistory | `HAS_PERIOD` | LogPeriod |
| MessageTemplate | `DRAFTED_IN` | CampaignDraft |
| MessageTemplate | `HAS_VERSION` | TemplateVersion |
| MetricHistory | `HAS_PERIOD` | LogPeriod |
| MissedConversation | `MISSED` | Conversation |
| NextBestActionSet | `CONTAINS` | NextBestAction |
| Note | `WRITTEN_BY` | Staff |
| Notification | `ABOUT_CAMPAIGN` | Campaign |
| Notification | `ABOUT_TEMPLATE` | RegisteredTemplate |
| Notification | `ABOUT_TICKET` | Ticket |
| PersonaEvidence | `EVIDENCE_FOR` | Persona |
| PhoneNumber | `ROUTES_TO` | Employee |
| PhoneNumber | `ROUTES_TO` | QueueSetup |
| PhoneNumber | `USES_PROMPTS` | VoicePromptSet |
| PriceList | `HAS_ENTRY` | PriceListEntry |
| PriceListEntry | `PRICE_FOR` | AddOn |
| PriceListEntry | `PRICE_FOR` | Product |
| PriceListEntry | `PRICE_FOR` | ProductPlan |
| PrivacyRequest | `ABOUT_CUSTOMER` | Contact |
| PrivacyRequest | `RAISED_IN` | ContactActivity |
| PrivacyRequest | `REVIEWED_BY` | Employee |
| ProactiveDecision | `DECIDED_ON` | ProactiveEvent |
| ProactiveHistory | `CONTAINS` | Approval |
| ProactiveHistory | `CONTAINS` | ProactiveDecision |
| ProactiveHistory | `CONTAINS` | Recommendation |
| Product | `IN_CATEGORY` | ProductCategory |
| Product | `IN_SUBCATEGORY` | ProductSubcategory |
| Product | `OF_BRAND` | Brand |
| ProductCatalog | `HAS_PRICE_LIST` | PriceList |
| ProductCatalog | `HAS_PRODUCT` | Product |
| ProductPlan | `PLAN_FOR` | Product |
| Proposal | `APPROVED_BY` | Employee |
| Proposal | `FOR_DEAL` | Deal |
| Proposal | `USES_TEMPLATE` | DocumentTemplate |
| Purchase | `INCLUDES` | Product |
| Queue | `COPY_OF` | QueueSetup |
| Queue | `FOLLOWS_HOURS` | BusinessHours |
| Queue | `HAS_MEMBER` | Staff |
| Queue | `HAS_ROUTING_HISTORY` | RoutingHistory |
| Queue | `HAS_ROUTING_POLICY` | RoutingPolicy |
| Queue | `HAS_ROUTING_PROFILE` | RoutingProfile |
| Queue | `OWNED_BY_TEAM` | Team |
| Queue | `REQUIRES_SKILL` | Skill |
| QueueSetup | `FOLLOWS_HOURS` | BusinessHours |
| QueueSetup | `HAS_MEMBER` | Employee |
| QueueSetup | `HAS_ROUTING_PROFILE` | RoutingProfile |
| QueueSetup | `OWNED_BY_TEAM` | Team |
| Quote | `APPROVED_BY` | Employee |
| Quote | `FOR_DEAL` | Deal |
| Quote | `FROM_PROPOSAL` | Proposal |
| Quote | `HAS_LINE` | LineItem |
| Quote | `RENEWS` | Contract |
| RegisteredTemplate | `FOR_CAMPAIGN` | Campaign |
| RegisteredTemplate | `FOR_CHANNEL` | Channel |
| Role | `GIVES_ACCESS_TO` | PlatformApp |
| RoutingAttempt | `ATTEMPT_FOR` | Conversation |
| RoutingHistory | `HAS_PERIOD` | LogPeriod |
| RoutingPolicy | `OVERFLOWS_TO` | Queue |
| SalesOrder | `FOR_ACCOUNT` | Account |
| SalesOrder | `FOR_DEAL` | Deal |
| SalesOrder | `FROM_QUOTE` | Quote |
| SalesOrder | `HAS_LINE` | LineItem |
| SalesOrder | `PLACED_BY` | Contact |
| SalesProfile | `AT_LOCATION` | AccountLocation |
| SalesProfile | `CONVERTED_TO` | Deal |
| SalesProfile | `OWNED_BY` | Employee |
| SalesProfile | `SALES_VIEW_OF` | Contact |
| SalesProfile | `WORKS_AT` | Account |
| SendBatch | `COMPLIES_WITH` | SenderProfile |
| SendBatch | `LIMITED_BY` | SendingLimit |
| SenderEmail | `VERIFIED_ON` | EmailDomain |
| Session | `ON_WEBSITE` | Website |
| SessionHistory | `HAS_PERIOD` | LogPeriod |
| SetupHub | `HAS_ASSIGNMENT_POLICY` | AssignmentPolicy |
| SetupHub | `HAS_BUSINESS_HOURS` | BusinessHours |
| SetupHub | `HAS_BUSINESS_PROFILE` | BusinessProfile |
| SetupHub | `HAS_CONNECTED_CHANNEL` | ConnectedChannel |
| SetupHub | `HAS_EMPLOYEE` | Employee |
| SetupHub | `HAS_HOLIDAY_CALENDAR` | HolidayCalendar |
| SetupHub | `HAS_INDUSTRY_PACK` | IndustryPack |
| SetupHub | `HAS_INDUSTRY_PROFILE` | IndustryProfile |
| SetupHub | `HAS_KNOWLEDGE_FILE` | KnowledgeFile |
| SetupHub | `HAS_PHONE_NUMBER` | PhoneNumber |
| SetupHub | `HAS_QUEUE_SETUP` | QueueSetup |
| SetupHub | `HAS_ROLE` | Role |
| SetupHub | `HAS_SUBSCRIPTION` | Subscription |
| SetupHub | `HAS_TEAM` | Team |
| SetupHub | `HAS_TEMPLATE` | DocumentTemplate |
| SetupHub | `HAS_VOICE_PROMPT_SET` | VoicePromptSet |
| SlaTimer | `GOVERNED_BY` | SLA |
| Staff | `HAS_AVAILABILITY` | StaffAvailability |
| Staff | `HAS_CAPACITY` | ChannelCapacity |
| Staff | `ROLE_OF` | Employee |
| Staff | `SKILLED_IN` | Skill |
| Subscription | `FOR_APP` | PlatformApp |
| SupportHub | `HAS_ARTICLE` | KnowledgeArticle |
| SupportHub | `HAS_COMPANY` | Company |
| SupportHub | `HAS_MACRO` | Macro |
| SupportHub | `HAS_PORTAL_SETTINGS` | PortalSettings |
| SupportHub | `HAS_QUEUE` | Queue |
| SupportHub | `HAS_RULE` | AutomationRule |
| SupportHub | `HAS_SLA` | SLA |
| Task | `ABOUT_CONVERSATION` | Conversation |
| Task | `CREATED_BY` | Employee |
| Team | `FOLLOWS_HOURS` | BusinessHours |
| Ticket | `ASSIGNED_TO` | Staff |
| Ticket | `GOVERNED_BY` | SLA |
| Ticket | `HAS_NOTE` | Note |
| Ticket | `HAS_PROACTIVE_HISTORY` | ProactiveHistory |
| Ticket | `HAS_RESOLVER` | Staff |
| Ticket | `HAS_TICKET_HISTORY` | TicketHistory |
| Ticket | `RAISED_BY` | Contact |
| Ticket | `ROUTED_TO` | Queue |
| TicketHistory | `CONTAINS` | AuditEvent |
| TicketHistory | `CONTAINS` | OutboundEmail |
| Touchpoint | `OCCURS_IN` | Session |
| TouchpointHistory | `HAS_PERIOD` | LogPeriod |
| Transfer | `FROM_QUEUE` | Queue |
| Transfer | `TRANSFERS` | Conversation |
| Visitor | `IDENTIFIED_AS` | Contact |
| WebTrackingHub | `HAS_WEBSITE` | Website |
