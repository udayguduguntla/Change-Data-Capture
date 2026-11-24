# Core graph overview

The knowledge graph is for reports, analytics and investigation across products (the hub looking over every product's data). It keeps the business entities and how they connect; configuration, internal logs, copies of another product's data and high-volume metrics stay out.

**90 tables mapped · 97 node labels · 89 tables left out** (`config/archive/dropped_from_graph_v45.yaml`)

Reading rule: a node is readable by an agent when it has no product label, or one of the agent's products. `HAS` = containment; verbs = business meaning.

## CDP

| Table | Node | Key | Edges | Collection |
|---|---|---|---|---|
| `cdp.ai_insights` | `ContactInsight` | `insight_id` | — | Contact → **InsightHistory** |
| `cdp.contact_identities` | `ContactIdentity` *(adds properties)* | `uid` = `{organization_id}:{identity_type}:{identity_value}` | — | Contact → **ContactProfile** |
| `cdp.customer_persona_assignments` | *edge only* | — | (Contact)-[:MATCHES_PERSONA]->(Persona) | — |
| `cdp.customer_profile` | `Contact` | `cdp_id` | — | — |
| `cdp.customer_recommendations` | `NextBestAction` | `recommendation_id` | — | Contact → **NextBestActionHistory** |
| `cdp.customer_scores` | `Contact` *(adds properties)* | `cdp_id` | — | — |
| `cdp.customer_segments` | *edge only* | — | (Contact)-[:IN_SEGMENT]->(Segment) | — |
| `cdp.gold_customer_metrics` | `Contact` *(adds properties)* | `cdp_id` | — | — |
| `cdp.identity_keys` | `ContactIdentity`<br>*(identity_id→source_row_id, key_type→identity_type, key_value→identity_value)* | `uid` = `{organization_id}:{key_type}:{key_value}` | — | Contact → **ContactProfile** |
| `cdp.identity_links` | *edge only* | — | (Contact)-[:SAME_PERSON_AS]->(Contact) | — |
| `cdp.journey_states` | `JourneyStatus` | `uid` = `{organization_id}:{cdp_id}:{journey_name}` | <-[:HAS]-Contact | — |
| `cdp.order_items` | *edge only*<br>*(subtotal_amount→subtotal)* | — | (Product)-[:HAS]->(Purchase) | Product → **ProductPurchaseHistory** |
| `cdp.orders` | `Purchase` | `order_id` | — | Contact → **PurchaseHistory** (by month) |
| `cdp.persona_definitions` | `Persona` | `persona_id` | — | — |
| `cdp.segments` | `Segment` | `segment_id` | — | — |

## ContactCentre

| Table | Node | Key | Edges | Collection |
|---|---|---|---|---|
| `ccass.agent_missed_contacts` | `MissedContact` | `id` | <-[:HAS]-Conversation<br>-[:MISSED_BY]->ContactCentreAgent<br>-[:IN_QUEUE]->ContactCentreQueue | — |
| `ccass.agent_presence` | `ContactCentreAgent` *(adds properties)* | `id` | — | — |
| `ccass.agent_skills` | *edge only* | — | (ContactCentreAgent)-[:SKILLED_IN]->(Skill) | — |
| `ccass.agent_tasks` | `Task` | `id` | <-[:HAS]-Conversation<br>-[:ASSIGNED_TO]->ContactCentreAgent<br>-[:CREATED_BY]->User | — |
| `ccass.agents` | `ContactCentreAgent` | `id` | <-[:ACTS_AS]-User | — |
| `ccass.conversation_memory` | `Conversation` *(adds properties)*<br>*(rolling_summary→summary, extracted_facts→facts, updated_at→summary_updated_at)* | `id` | — | — |
| `ccass.conversation_notes` | `InternalNote` | `uid` = `conversation:{id}` | <-[:HAS]-Conversation<br>-[:WRITTEN_BY]->ContactCentreAgent | — |
| `ccass.conversation_transfers` | `Transfer` | `id` | <-[:HAS]-Conversation<br>-[:FROM_QUEUE]->ContactCentreQueue<br>-[:TO_QUEUE]->ContactCentreQueue<br>-[:FROM_AGENT]->ContactCentreAgent<br>-[:OFFERED_TO]->ContactCentreAgent | — |
| `ccass.conversations` | `Conversation` | `id` | -[:ROUTED_TO]->ContactCentreQueue<br>-[:HANDLED_BY]->ContactCentreAgent<br>-[:VIA_CHANNEL]->Channel<br>-[:CONTINUES]->Conversation<br>-[:VIA_CHANNEL]->Channel<br>-[:VIA_CHANNEL]->Channel | Contact → **ConversationHistory** |
| `ccass.queue_members` | *edge only* | — | (ContactCentreAgent)-[:MEMBER_OF]->(ContactCentreQueue) | — |
| `ccass.queue_routing_profiles` | `ContactCentreQueue` *(adds properties)* | `id` | — | — |
| `ccass.queue_skills` | *edge only* | — | (ContactCentreQueue)-[:REQUIRES_SKILL]->(Skill) | — |
| `ccass.queues` | `ContactCentreQueue` | `id` | -[:OWNED_BY]->Team<br>-[:FOLLOWS]->BusinessHours | — |
| `ccass.skills` | `Skill` | `id` | — | — |
| `ccass.sla_policies` | `ContactCentreSLA` | `uid` = `service:{id}` | — | — |

## Support

| Table | Node | Key | Edges | Collection |
|---|---|---|---|---|
| `ticketing.agent_queue_members` | *edge only* | — | (SupportAgent)-[:MEMBER_OF]->(SupportQueue) | — |
| `ticketing.agent_queues` | `SupportQueue` | `id` | — | — |
| `ticketing.agents` | `SupportAgent` | `id` | <-[:ACTS_AS]-User | — |
| `ticketing.audit_events` | `AuditEvent` | `id` | -[:PERFORMED_BY]->SupportAgent | Ticket → **TicketAuditTrail** |
| `ticketing.contact_profile` | `None` | `id` | — | — |
| `ticketing.conversation_contexts` | `Session` | `uid` = `ticketing:{id}` | -[:USED_BY]->SupportAgent | — |
| `ticketing.email_message_refs` | `InboundEmail` | `id` | <-[:HAS]-Ticket | — |
| `ticketing.kb_articles` | `KnowledgeArticle` | `id` | -[:WRITTEN_BY]->SupportAgent | — |
| `ticketing.macros` | `Macro` | `id` | -[:CREATED_BY]->SupportAgent | Tenant → **MacroLibrary** |
| `ticketing.outbound_email_log` | `OutboundEmail` | `id` | — | Ticket → **TicketAuditTrail** |
| `ticketing.proactive_approvals` | `Approval` | `id` | -[:DECIDED_BY]->SupportAgent | Ticket → **ProactiveHistory** |
| `ticketing.proactive_recommendations` | `Recommendation` | `id` | -[:SHOWN_TO]->SupportAgent | Ticket → **ProactiveHistory** |
| `ticketing.sla_policies` | `SupportSLA` | `uid` = `support:{id}` | — | — |
| `ticketing.ticket_notes` | `Note` | `id` | <-[:HAS]-Ticket<br>-[:WRITTEN_BY]->SupportAgent | — |
| `ticketing.ticketing_accounts` | `SupportCompany` | `id` | — | — |
| `ticketing.tickets` | `Ticket` | `id` | -[:ROUTED_TO]->SupportQueue<br>-[:ASSIGNED_TO]->SupportAgent<br>-[:RESOLVED_BY]->SupportAgent<br>-[:GOVERNED_BY]->SupportSLA<br>-[:FOR_COMPANY]->SupportCompany | Contact → **TicketHistory** |

## Sales

| Table | Node | Key | Edges | Collection |
|---|---|---|---|---|
| `crm.appointments` | `Meeting` | `id` | <-[:HAS]-ContactActivity<br>-[:HOSTED_BY]->SalesRep *(role of a User)* | — |
| `crm.companies` | `Account` | `id` | -[:OWNED_BY]->SalesRep *(role of a User)*<br>-[:PART_OF]->Account | — |
| `crm.contact_activities` | `ContactActivity` | `id` | -[:ASSIGNED_TO]->SalesRep *(role of a User)* | Contact → **ActivityHistory** |
| `crm.contact_profiles` | `SalesProfile` | `id` | <-[:HAS]-Contact<br>-[:WORKS_AT]->Account<br>-[:OWNED_BY]->SalesRep *(role of a User)*<br>-[:CONVERTED_TO]->Deal | — |
| `crm.contracts` | `Contract` | `id` | -[:FROM_DEAL]->Deal<br>-[:RENEWED_BY]->Deal | Account → **AccountContractHistory** |
| `crm.deal_activities` | `DealActivity` | `id` | <-[:HAS]-Deal<br>-[:CREATED_BY]->SalesRep *(role of a User)* | — |
| `crm.deals` | `Deal` | `id` | -[:OWNED_BY]->SalesRep *(role of a User)* | Contact → **DealHistory**<br>Account → **AccountDealHistory** |
| `crm.invoice_lines` | `LineItem` | `uid` = `invoice:{id}` | <-[:HAS]-Invoice<br>-[:FOR_PRODUCT]->Product | — |
| `crm.invoices` | `Invoice` | `id` | -[:FOR_ORDER]->SalesOrder<br>-[:UNDER_CONTRACT]->Contract<br>-[:APPROVED_BY]->SalesRep *(role of a User)* | Account → **AccountInvoiceHistory** |
| `crm.lead_summaries` | `LeadSummary` | `id` | <-[:HAS]-Contact | — |
| `crm.meeting_analyses` | `MeetingOutcome` | `id` | <-[:HAS]-ContactActivity<br>-[:FOR_DEAL]->Deal | — |
| `crm.meeting_assist_sessions` | `Session` | `uid` = `crm:{id}` | <-[:HAS]-ContactActivity<br>-[:HOSTED_BY]->SalesRep *(role of a User)* | — |
| `crm.order_lines` | `LineItem` | `uid` = `order:{id}` | <-[:HAS]-SalesOrder<br>-[:FOR_PRODUCT]->Product | — |
| `crm.privacy_requests` | `PrivacyRequest` | `id` | <-[:HAS]-Contact<br>-[:RAISED_IN]->ContactActivity<br>-[:REVIEWED_BY]->SalesRep *(role of a User)* | — |
| `crm.proposals` | `Proposal` | `id` | <-[:HAS]-Deal<br>-[:APPROVED_BY]->SalesRep *(role of a User)* | — |
| `crm.quote_lines` | `LineItem` | `uid` = `quote:{id}` | <-[:HAS]-Quote<br>-[:FOR_PRODUCT]->Product | — |
| `crm.quotes` | `Quote` | `id` | <-[:HAS]-Deal<br>-[:FROM_PROPOSAL]->Proposal<br>-[:RENEWS]->Contract<br>-[:APPROVED_BY]->SalesRep *(role of a User)* | — |
| `crm.sales_orders` | `SalesOrder` | `id` | <-[:HAS]-Deal<br>-[:FROM_QUOTE]->Quote | Contact → **OrderHistory**<br>Account → **AccountOrderHistory** |
| `crm.sales_user_assignment_profiles` | `SalesRep` | `user_id` | <-[:ACTS_AS]-User | — |

## Marketing

| Table | Node | Key | Edges | Collection |
|---|---|---|---|---|
| `campaign.campaign_agent_sessions` | `Session` | `uid` = `campaign:{id}` | -[:CREATED_BY]->MarketingAgent *(role of a User)* | — |
| `campaign.campaign_audience` | *edge only* | — | (Contact)-[:TARGETED_BY]->(Campaign) | Contact → **CampaignHistory** |
| `campaign.campaign_channel_templates` | `MessageTemplate` | `id` | <-[:HAS]-Session | — |
| `campaign.campaign_contact_engagement` | `EmailEngagement` | `id` | <-[:HAS]-Contact | — |
| `campaign.campaign_send_recipients` | `Delivery` | `id` | -[:FOR_CAMPAIGN]->Campaign | Contact → **DeliveryHistory** (by month) |
| `campaign.campaigns` | `Campaign` | `id` | -[:CREATED_BY]->MarketingAgent *(role of a User)*<br>-[:FROM_SESSION]->Session | — |

## WebTracking

| Table | Node | Key | Edges | Collection |
|---|---|---|---|---|
| `cdp.customer_events` | `Event` | `event_id` | <-[:HAS]-Session<br>-[:ON_WEBSITE]->Website | Contact → **EventHistory** (by month) |
| `cdp.customer_sessions` | `Session` | `uid` = `web:{session_id}` | -[:ON_WEBSITE]->Website<br>-[:FROM_CAMPAIGN]->Campaign | Contact → **SessionHistory** (by month) |
| `digital.tracked_sites` | `Website` | `site_id` | — | — |

## (no product label)

| Table | Node | Key | Edges | Collection |
|---|---|---|---|---|
| `accounts.organizations` | `Tenant` | `id` | — | — |
| `accounts.setup_business_hour_shifts` | `Shift` | `id` | <-[:HAS]-BusinessHours | — |
| `accounts.setup_business_hours` | `BusinessHours` | `id` | -[:FOLLOWS]->HolidayCalendar | — |
| `accounts.setup_channel_configs` | `Channel` | `id` | — | Tenant → **ChannelDirectory** |
| `accounts.setup_email_configs` | `Channel` *(adds properties)*<br>*(provider_type→email_provider_type, sender_email→email_sender_address, sender_name→email_sender_name, reply_to→email_reply_to, domain→email_domain, oauth_provider→email_oauth_provider, ccaas_handler→email_service_handler, is_bot→email_bot_mailbox, provisioning_status→email_provisioning_status, oauth_connection_id→email_connection_id)* | `id` | — | — |
| `accounts.setup_holiday_calendars` | `HolidayCalendar` | `id` | — | — |
| `accounts.setup_holidays` | `Holiday` | `id` | <-[:HAS]-HolidayCalendar | — |
| `accounts.setup_knowledge_documents` | `KnowledgeDocument` | `id` | -[:UPLOADED_BY]->User | Tenant → **KnowledgeLibrary** |
| `accounts.setup_knowledge_org_profiles` | `Tenant` *(adds properties)* | `id` | — | — |
| `accounts.setup_roles` | `Role` | `id` | — | — |
| `accounts.setup_social_configs` | `Channel` *(adds properties)*<br>*(provider→social_provider, bot_enabled→social_bot_enabled, usage_mode→social_usage_mode, display_number→social_display_number, waba_id→social_waba_id, phone_number_id→social_phone_number_id, page_id→social_page_id, ig_business_account_id→social_ig_account_id, meta_auth_type→social_auth_type, meta_connection_status→social_connection_status)* | `id` | — | — |
| `accounts.setup_teams` | `Team` | `id` | -[:FOLLOWS]->BusinessHours | Tenant → **TeamDirectory** |
| `accounts.setup_users` | `User` | `id` | -[:MEMBER_OF]->Team<br>-[:FOLLOWS]->BusinessHours | Tenant → **UserDirectory** |
| `accounts.setup_voice_dids` | `PhoneNumber` | `id` | -[:ROUTES_TO]->ContactCentreQueue<br>-[:ROUTES_TO]->User | — |
| `accounts.setup_webchat_configs` | `Channel` *(adds properties)*<br>*(webchat_id→chat_widget_id, title→chat_title, alias_title→chat_alias_title, description→chat_description, greeting_message→chat_greeting, trigger_message→chat_trigger)* | `id` | -[:ROUTES_TO]->ContactCentreQueue | — |
| `inventory.products` | `Product` | `id` | — | Tenant → **ProductCatalog** |

## Collection owners

- **Account**: AccountDealHistory, AccountOrderHistory, AccountInvoiceHistory, AccountContractHistory
- **Contact**: ContactProfile, EventHistory, SessionHistory, InsightHistory, NextBestActionHistory, PurchaseHistory, ConversationHistory, DealHistory, ActivityHistory, OrderHistory, TicketHistory, CampaignHistory, DeliveryHistory
- **Product**: ProductPurchaseHistory
- **Tenant**: UserDirectory, TeamDirectory, ChannelDirectory, KnowledgeLibrary, ProductCatalog, MacroLibrary
- **Ticket**: TicketAuditTrail, ProactiveHistory

## Embedded (searchable by meaning)

- `Campaign`: audience_summary, description, goal, name, objective, source_channel
- `Contact`: city, company, country, first_name, last_name
- `Conversation`: channel, resolution_summary, subject, summary
- `Deal`: lost_reason, notes, title
- `Event`: channel, event_category, event_name, page_title
- `InternalNote`: body
- `KnowledgeArticle`: category, content, title
- `KnowledgeDocument`: category, file_name, product_id
- `Macro`: body, title
- `MessageTemplate`: channel, how_it_works
- `NextBestAction`: recommendation
- `Persona`: industry, name, persona_type, summary
- `Product`: brand, category, description, name, subcategory, tagline
- `Recommendation`: reason
- `SalesProfile`: notes
- `Tenant`: business_type, company_name, country, industry_type
- `Ticket`: ai_summary, category, description, subject

## Computed and looked-up columns

- `ticketing.tickets.customer_cdp_id` = `ticketing.contact_profile.cdp_id` where `id` = row `customer_id`
- `cdp.order_items.product_sku` = `cdp.products.sku` where `product_id` = row `product_id`, same `organization_id`
- `cdp.order_items.inventory_product_id` = `inventory.products.id` where `sku` = row `product_sku`, same `organization_id`
- `cdp.customer_profile.campaign_contact_id` = `campaign_contact_id(cdp_id)`
- `ticketing.tickets.customer_company_id` = `ticketing.contact_profile.account_id` where `id` = row `customer_id`, same `organization_id`

## Left out of the graph

- **accounts** (5): setup_ccaas_queue_agents, setup_ccaas_queues, setup_queue_routing_profiles, setup_user_product_access, setup_voice_prompt_profiles
- **campaign** (21): app_notifications, branches, campaign_audience_refs, campaign_channels, campaign_recommendations, campaign_send_batches, campaign_template_messages, channel_sending_profiles, email_domains, email_org_profiles, email_return_paths, email_senders, email_sending_caps, email_suppressions, organization_products, push_channel_settings, push_endpoints, regulated_channel_templates, sms_channel_settings, tenant_industry_packs, website_contacts
- **ccass** (21): agent_capacity, channel_automation_policies, conversation_assignments, conversation_dispositions, conversation_message_attachments, conversation_messages, conversation_participants, conversation_sla_state, dispositions, email_mailbox_policies, nlp_conversations, nlp_skill_bindings, nlp_skill_tools, nlp_skill_versions, nlp_skills, presence_reason_codes, routing_attempts, routing_policies, webchat_sessions, whatsapp_channel_policies, workspace_common_notes
- **cdp** (15): activation_tasks, agentic_model, anomaly_alerts, anomaly_rules, anomaly_threshold_state, customer_interactions, customer_journey, customer_metrics, episodic_memory, gold_campaign_lookup, interactions, persona_snapshots, procedural_memory, semantic_memory, source_systems
- **crm** (22): agent_memories, coaching_insights, company_locations, contract_billing_schedules, invoice_templates, lead_enrichment_jobs, lead_enrichment_snapshots, lead_lifecycle_jobs, lead_nurture_jobs, lead_scoring_jobs, meeting_transcript_chunks, notifications, org_assignment_settings, organization_profiles, price_book_entries, price_books, process_confirmations, product_addon_tiers, product_addons, product_offers, proposal_templates, quote_templates
- **ticketing** (5): automation_rules, notifications, org_settings, proactive_audit, workflow_events
