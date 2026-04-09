/**
 * ============================================================
 * SEO AUDIT — ALL SEQUELIZE MODELS (PostgreSQL)
 * ============================================================
 * Copy this file as /models/seoModels.js in your Node.js app
 * Usage: const { SeoAudit, SeoPage, ... } = require('./models/seoModels');
 */

const { DataTypes } = require("sequelize");
const { sequelize }  = require("../config/database"); // adjust path as needed

// ── Audit (master record, one per crawl run) ───────────────────────────────
const SeoAudit = sequelize.define("SeoAudit", {
  id:                  { type: DataTypes.INTEGER,      primaryKey: true, autoIncrement: true },
  brand_id:            { type: DataTypes.INTEGER,      allowNull: false },
  domain:              { type: DataTypes.TEXT,         allowNull: true },
  base_url:            { type: DataTypes.TEXT,         allowNull: true },
  target_location:     { type: DataTypes.STRING(255),  allowNull: true, defaultValue: "Global" },
  business_type:       { type: DataTypes.TEXT,         allowNull: true },
  ai_mode:             { type: DataTypes.STRING(10),   allowNull: true, defaultValue: "1" },
  total_pages_crawled: { type: DataTypes.INTEGER,      allowNull: true, defaultValue: 0 },
  pages_200:           { type: DataTypes.INTEGER,      allowNull: true, defaultValue: 0 },
  pages_404:           { type: DataTypes.INTEGER,      allowNull: true, defaultValue: 0 },
  broken_links_count:  { type: DataTypes.INTEGER,      allowNull: true, defaultValue: 0 },
  images_missing_alt:  { type: DataTypes.INTEGER,      allowNull: true, defaultValue: 0 },
  robots_txt_status:   { type: DataTypes.TEXT,         allowNull: true },
  sitemap_status:      { type: DataTypes.TEXT,         allowNull: true },
  llm_txt_status:      { type: DataTypes.TEXT,         allowNull: true },
  gbp_status:          { type: DataTypes.TEXT,         allowNull: true },
  site_recommendation: { type: DataTypes.TEXT,         allowNull: true },
  detected_location:   { type: DataTypes.TEXT,         allowNull: true },
  excel_file:          { type: DataTypes.TEXT,         allowNull: true },
  pdf_file:            { type: DataTypes.TEXT,         allowNull: true },
  audit_status:        { type: DataTypes.STRING(50),   allowNull: true, defaultValue: "in_progress" },
  audit_timestamp:     { type: DataTypes.DATE,         allowNull: true, defaultValue: DataTypes.NOW },
}, { tableName: "audits", timestamps: false });

// ── Pages (one row per crawled URL) ───────────────────────────────────────
const SeoPage = sequelize.define("SeoPage", {
  id:                       { type: DataTypes.INTEGER,      primaryKey: true, autoIncrement: true },
  audit_id:                 { type: DataTypes.INTEGER,      allowNull: false },
  url:                      { type: DataTypes.TEXT,         allowNull: true },
  url_cleaned:              { type: DataTypes.TEXT,         allowNull: true },
  status:                   { type: DataTypes.STRING(100),  allowNull: true },
  redirect_suggestion:      { type: DataTypes.TEXT,         allowNull: true },
  redirect_type:            { type: DataTypes.STRING(20),   allowNull: true },
  redirect_target:          { type: DataTypes.TEXT,         allowNull: true },
  canonical_status:         { type: DataTypes.STRING(100),  allowNull: true },
  canonical_url:            { type: DataTypes.TEXT,         allowNull: true },
  duplicate_status:         { type: DataTypes.TEXT,         allowNull: true },
  word_count:               { type: DataTypes.INTEGER,      allowNull: true, defaultValue: 0 },
  thin_content:             { type: DataTypes.STRING(10),   allowNull: true },
  current_title:            { type: DataTypes.TEXT,         allowNull: true },
  title_length:             { type: DataTypes.INTEGER,      allowNull: true, defaultValue: 0 },
  current_meta_description: { type: DataTypes.TEXT,         allowNull: true },
  meta_desc_length:         { type: DataTypes.INTEGER,      allowNull: true, defaultValue: 0 },
  current_h1:               { type: DataTypes.TEXT,         allowNull: true },
  h2_tags:                  { type: DataTypes.TEXT,         allowNull: true },
  google_analytics:         { type: DataTypes.STRING(10),   allowNull: true },
  google_search_console:    { type: DataTypes.STRING(50),   allowNull: true },
  og_tags:                  { type: DataTypes.STRING(20),   allowNull: true },
  og_title_current:         { type: DataTypes.TEXT,         allowNull: true },
  og_description_current:   { type: DataTypes.TEXT,         allowNull: true },
  og_image_current:         { type: DataTypes.TEXT,         allowNull: true },
  schema_markup:            { type: DataTypes.STRING(20),   allowNull: true },
  schema_types_found:       { type: DataTypes.TEXT,         allowNull: true },
  total_images:             { type: DataTypes.INTEGER,      allowNull: true, defaultValue: 0 },
  images_missing_alt:       { type: DataTypes.INTEGER,      allowNull: true, defaultValue: 0 },
  image_alt_status:         { type: DataTypes.TEXT,         allowNull: true },
  primary_keyword:          { type: DataTypes.STRING(500),  allowNull: true },
  secondary_keywords:       { type: DataTypes.TEXT,         allowNull: true },
  short_tail_keywords:      { type: DataTypes.TEXT,         allowNull: true },
  long_tail_keywords:       { type: DataTypes.TEXT,         allowNull: true },
  ai_meta_title:            { type: DataTypes.TEXT,         allowNull: true },
  ai_meta_description:      { type: DataTypes.TEXT,         allowNull: true },
  ai_h1:                    { type: DataTypes.TEXT,         allowNull: true },
  ai_og_title:              { type: DataTypes.TEXT,         allowNull: true },
  ai_og_description:        { type: DataTypes.TEXT,         allowNull: true },
  ai_og_image_url:          { type: DataTypes.TEXT,         allowNull: true },
  ai_schema_recommendation: { type: DataTypes.STRING(255),  allowNull: true },
  ai_schema_code_snippet:   { type: DataTypes.TEXT,         allowNull: true },
  ai_optimized_url:         { type: DataTypes.TEXT,         allowNull: true },
  image_optimization_tips:  { type: DataTypes.TEXT,         allowNull: true },
  serp_preview:             { type: DataTypes.TEXT,         allowNull: true },
  mobile_score:             { type: DataTypes.STRING(20),   allowNull: true },
  mobile_lcp:               { type: DataTypes.STRING(50),   allowNull: true },
  mobile_cls:               { type: DataTypes.STRING(50),   allowNull: true },
  mobile_fcp:               { type: DataTypes.STRING(50),   allowNull: true },
  desktop_score:            { type: DataTypes.STRING(20),   allowNull: true },
  desktop_lcp:              { type: DataTypes.STRING(50),   allowNull: true },
  desktop_cls:              { type: DataTypes.STRING(50),   allowNull: true },
  desktop_fcp:              { type: DataTypes.STRING(50),   allowNull: true },
  seo_score:                { type: DataTypes.INTEGER,      allowNull: true, defaultValue: 0 },
  seo_grade:                { type: DataTypes.STRING(10),   allowNull: true },
  spam_malware_flags:       { type: DataTypes.TEXT,         allowNull: true },
  aeo_faq:                  { type: DataTypes.TEXT,         allowNull: true },
  body_copy_guidance:       { type: DataTypes.TEXT,         allowNull: true },
  viewport_configured:      { type: DataTypes.STRING(10),   allowNull: true },
  html_size_kb:             { type: DataTypes.DECIMAL(10,2),allowNull: true },
  html_size_issue:          { type: DataTypes.STRING(10),   allowNull: true },
  is_secure:                { type: DataTypes.STRING(10),   allowNull: true },
  mixed_content:            { type: DataTypes.STRING(10),   allowNull: true },
  mixed_content_details:    { type: DataTypes.TEXT,         allowNull: true },
  unminified_js:            { type: DataTypes.STRING(10),   allowNull: true },
  unminified_js_details:    { type: DataTypes.TEXT,         allowNull: true },
  unminified_css:           { type: DataTypes.STRING(10),   allowNull: true },
  unminified_css_details:   { type: DataTypes.TEXT,         allowNull: true },
  amp_link:                 { type: DataTypes.TEXT,         allowNull: true },
  og_validation:            { type: DataTypes.TEXT,         allowNull: true },
  x_robots_noindex:         { type: DataTypes.STRING(10),   allowNull: true },
  page_cache_control:       { type: DataTypes.TEXT,         allowNull: true },
  crawl_depth:              { type: DataTypes.INTEGER,      allowNull: true, defaultValue: -1 },
  hreflang_tags:            { type: DataTypes.TEXT,         allowNull: true },
  created_at:               { type: DataTypes.DATE,         allowNull: true, defaultValue: DataTypes.NOW },
}, { tableName: "pages", timestamps: false });

// ── Scorecard ──────────────────────────────────────────────────────────────
const SeoScorecard = sequelize.define("SeoScorecard", {
  id:          { type: DataTypes.INTEGER,     primaryKey: true, autoIncrement: true },
  audit_id:    { type: DataTypes.INTEGER,     allowNull: false },
  parameter:   { type: DataTypes.STRING(255), allowNull: true },
  pass_count:  { type: DataTypes.INTEGER,     allowNull: true, defaultValue: 0 },
  fail_count:  { type: DataTypes.INTEGER,     allowNull: true, defaultValue: 0 },
  total_count: { type: DataTypes.INTEGER,     allowNull: true, defaultValue: 0 },
  pass_rate:   { type: DataTypes.FLOAT,       allowNull: true, defaultValue: 0 },
  status:      { type: DataTypes.STRING(50),  allowNull: true },
  check_type:  { type: DataTypes.STRING(50),  allowNull: true, defaultValue: "per_page" },
}, { tableName: "scorecard", timestamps: false });

// ── SEO Keywords ───────────────────────────────────────────────────────────
const SeoKeyword = sequelize.define("SeoKeyword", {
  id:                  { type: DataTypes.INTEGER,     primaryKey: true, autoIncrement: true },
  audit_id:            { type: DataTypes.INTEGER,     allowNull: false },
  service_name:        { type: DataTypes.STRING(255), allowNull: true },
  keyword:             { type: DataTypes.STRING(255), allowNull: true },
  keyword_type:        { type: DataTypes.STRING(100), allowNull: true },
  primary_keyword:     { type: DataTypes.STRING(255), allowNull: true },
  secondary_keywords:  { type: DataTypes.TEXT,        allowNull: true },
  short_tail_keywords: { type: DataTypes.TEXT,        allowNull: true },
  long_tail_keywords:  { type: DataTypes.TEXT,        allowNull: true },
}, { tableName: "seo_keywords", timestamps: false });

// ── Blog Topics ────────────────────────────────────────────────────────────
const SeoBlogTopic = sequelize.define("SeoBlogTopic", {
  id:             { type: DataTypes.INTEGER,     primaryKey: true, autoIncrement: true },
  audit_id:       { type: DataTypes.INTEGER,     allowNull: false },
  service_name:   { type: DataTypes.STRING(255), allowNull: true },
  title:          { type: DataTypes.TEXT,        allowNull: true },
  topic_type:     { type: DataTypes.STRING(100), allowNull: true },
  target_keyword: { type: DataTypes.STRING(255), allowNull: true },
  description:    { type: DataTypes.TEXT,        allowNull: true },
}, { tableName: "blog_topics", timestamps: false });

// ── Backlink Strategies ────────────────────────────────────────────────────
const SeoBacklink = sequelize.define("SeoBacklink", {
  id:             { type: DataTypes.INTEGER,     primaryKey: true, autoIncrement: true },
  audit_id:       { type: DataTypes.INTEGER,     allowNull: false },
  category:       { type: DataTypes.STRING(100), allowNull: true },
  strategy_name:  { type: DataTypes.STRING(255), allowNull: true },
  description:    { type: DataTypes.TEXT,        allowNull: true },
  priority:       { type: DataTypes.STRING(50),  allowNull: true },
  difficulty:     { type: DataTypes.STRING(50),  allowNull: true },
  target_domains: { type: DataTypes.TEXT,        allowNull: true },
}, { tableName: "backlink_strategies", timestamps: false });

// ── 6-Month Plan ───────────────────────────────────────────────────────────
const SeoSixMonthPlan = sequelize.define("SeoSixMonthPlan", {
  id:              { type: DataTypes.INTEGER,     primaryKey: true, autoIncrement: true },
  audit_id:        { type: DataTypes.INTEGER,     allowNull: false },
  month_number:    { type: DataTypes.INTEGER,     allowNull: true },
  month_label:     { type: DataTypes.STRING(100), allowNull: true },
  theme:           { type: DataTypes.STRING(255), allowNull: true },
  tasks:           { type: DataTypes.TEXT,        allowNull: true }, // JSON array
  expected_output: { type: DataTypes.TEXT,        allowNull: true }, // JSON object
  kpis:            { type: DataTypes.TEXT,        allowNull: true }, // JSON array
}, { tableName: "six_month_plan", timestamps: false });

// ── Broken Links ───────────────────────────────────────────────────────────
const SeoBrokenLink = sequelize.define("SeoBrokenLink", {
  id:                  { type: DataTypes.INTEGER, primaryKey: true, autoIncrement: true },
  audit_id:            { type: DataTypes.INTEGER, allowNull: false },
  source_page:         { type: DataTypes.TEXT,    allowNull: true },
  broken_url:          { type: DataTypes.TEXT,    allowNull: true },
  status:              { type: DataTypes.STRING(20), allowNull: true },
  redirect_suggestion: { type: DataTypes.TEXT,    allowNull: true },
  redirect_type:       { type: DataTypes.STRING(10), allowNull: true, defaultValue: "301" },
}, { tableName: "broken_links", timestamps: false });

// ── Images ─────────────────────────────────────────────────────────────────
const SeoImage = sequelize.define("SeoImage", {
  id:                    { type: DataTypes.INTEGER, primaryKey: true, autoIncrement: true },
  audit_id:              { type: DataTypes.INTEGER, allowNull: false },
  page_url:              { type: DataTypes.TEXT,    allowNull: true },
  image_src:             { type: DataTypes.TEXT,    allowNull: true },
  alt_status:            { type: DataTypes.STRING(50), allowNull: true },
  current_alt:           { type: DataTypes.TEXT,    allowNull: true },
  ai_alt_recommendation: { type: DataTypes.TEXT,    allowNull: true },
}, { tableName: "images", timestamps: false });

// ── AEO FAQ ────────────────────────────────────────────────────────────────
const SeoFaq = sequelize.define("SeoFaq", {
  id:              { type: DataTypes.INTEGER, primaryKey: true, autoIncrement: true },
  audit_id:        { type: DataTypes.INTEGER, allowNull: false },
  page_url:        { type: DataTypes.TEXT,    allowNull: true },
  primary_keyword: { type: DataTypes.STRING(255), allowNull: true },
  question:        { type: DataTypes.TEXT,    allowNull: true },
  answer:          { type: DataTypes.TEXT,    allowNull: true },
}, { tableName: "aeo_faq", timestamps: false });

// ── AXO Recommendations ────────────────────────────────────────────────────
const SeoAxo = sequelize.define("SeoAxo", {
  id:             { type: DataTypes.INTEGER, primaryKey: true, autoIncrement: true },
  audit_id:       { type: DataTypes.INTEGER, allowNull: false },
  axo_score:      { type: DataTypes.INTEGER, allowNull: true, defaultValue: 0 },
  axo_grade:      { type: DataTypes.STRING(10), allowNull: true },
  category:       { type: DataTypes.STRING(100), allowNull: true },
  action_text:    { type: DataTypes.TEXT,    allowNull: true },
  priority:       { type: DataTypes.STRING(50), allowNull: true },
  impact:         { type: DataTypes.TEXT,    allowNull: true },
  implementation: { type: DataTypes.TEXT,    allowNull: true },
}, { tableName: "axo_recommendations", timestamps: false });

// ── Internal Linking ───────────────────────────────────────────────────────
const SeoInternalLink = sequelize.define("SeoInternalLink", {
  id:          { type: DataTypes.INTEGER, primaryKey: true, autoIncrement: true },
  audit_id:    { type: DataTypes.INTEGER, allowNull: false },
  entry_type:  { type: DataTypes.STRING(50),  allowNull: true }, // hub_page | link | silo
  from_url:    { type: DataTypes.TEXT,        allowNull: true },
  to_url:      { type: DataTypes.TEXT,        allowNull: true },
  anchor_text: { type: DataTypes.STRING(255), allowNull: true },
  context:     { type: DataTypes.TEXT,        allowNull: true },
  silo_name:   { type: DataTypes.STRING(255), allowNull: true },
  reason:      { type: DataTypes.TEXT,        allowNull: true },
}, { tableName: "internal_linking", timestamps: false });

// ── Keyword-URL Mapping ────────────────────────────────────────────────────
const SeoKeywordUrlMap = sequelize.define("SeoKeywordUrlMap", {
  id:               { type: DataTypes.INTEGER, primaryKey: true, autoIncrement: true },
  audit_id:         { type: DataTypes.INTEGER, allowNull: false },
  keyword:          { type: DataTypes.STRING(255), allowNull: true },
  keyword_type:     { type: DataTypes.STRING(100), allowNull: true },
  service_name:     { type: DataTypes.STRING(255), allowNull: true },
  mapped_url:       { type: DataTypes.TEXT,        allowNull: true },
  match_confidence: { type: DataTypes.STRING(50),  allowNull: true },
  reason:           { type: DataTypes.TEXT,        allowNull: true },
  on_page_action:   { type: DataTypes.TEXT,        allowNull: true },
  create_new_page:  { type: DataTypes.BOOLEAN,     allowNull: true, defaultValue: false },
  suggested_new_url:{ type: DataTypes.TEXT,        allowNull: true },
}, { tableName: "keyword_url_mapping", timestamps: false });

// ── Site Analysis ──────────────────────────────────────────────────────────
const SeoSiteAnalysis = sequelize.define("SeoSiteAnalysis", {
  id:             { type: DataTypes.INTEGER, primaryKey: true, autoIncrement: true },
  audit_id:       { type: DataTypes.INTEGER, allowNull: false },
  analysis_type:  { type: DataTypes.STRING(100), allowNull: true }, // http_status | crawl_depth | hreflang_summary | sitemap_comparison
  analysis_key:   { type: DataTypes.STRING(255), allowNull: true },
  analysis_value: { type: DataTypes.TEXT,        allowNull: true },
  count_value:    { type: DataTypes.INTEGER,     allowNull: true, defaultValue: 0 },
}, { tableName: "site_analysis", timestamps: false });

// ── Generated Files (sitemap.xml, robots.txt, etc.) ───────────────────────
const SeoGeneratedFile = sequelize.define("SeoGeneratedFile", {
  id:           { type: DataTypes.INTEGER, primaryKey: true, autoIncrement: true },
  audit_id:     { type: DataTypes.INTEGER, allowNull: false },
  file_name:    { type: DataTypes.STRING(255), allowNull: true },
  file_type:    { type: DataTypes.STRING(100), allowNull: true },
  file_content: { type: DataTypes.TEXT,        allowNull: true },
  file_size:    { type: DataTypes.INTEGER,     allowNull: true, defaultValue: 0 },
}, { tableName: "generated_files", timestamps: false });

// ── Audit Progress (crawl resume tracking) ─────────────────────────────────
const SeoAuditProgress = sequelize.define("SeoAuditProgress", {
  id:           { type: DataTypes.INTEGER, primaryKey: true, autoIncrement: true },
  audit_id:     { type: DataTypes.INTEGER, allowNull: false },
  url:          { type: DataTypes.TEXT,    allowNull: true },
  phase:        { type: DataTypes.STRING(50), allowNull: true },  // crawled | analyzed
  status_code:  { type: DataTypes.STRING(20), allowNull: true },
  processed_at: { type: DataTypes.DATE,    allowNull: true, defaultValue: DataTypes.NOW },
}, { tableName: "audit_progress", timestamps: false });


// ── Associations ───────────────────────────────────────────────────────────
// All tables belong to one audit
[
  SeoPage, SeoScorecard, SeoKeyword, SeoBlogTopic, SeoBacklink,
  SeoSixMonthPlan, SeoBrokenLink, SeoImage, SeoFaq, SeoAxo,
  SeoInternalLink, SeoKeywordUrlMap, SeoSiteAnalysis,
  SeoGeneratedFile, SeoAuditProgress,
].forEach(Model => {
  SeoAudit.hasMany(Model, { foreignKey: "audit_id" });
  Model.belongsTo(SeoAudit, { foreignKey: "audit_id" });
});


// ── Exports ────────────────────────────────────────────────────────────────
module.exports = {
  SeoAudit,
  SeoPage,
  SeoScorecard,
  SeoKeyword,
  SeoBlogTopic,
  SeoBacklink,
  SeoSixMonthPlan,
  SeoBrokenLink,
  SeoImage,
  SeoFaq,
  SeoAxo,
  SeoInternalLink,
  SeoKeywordUrlMap,
  SeoSiteAnalysis,
  SeoGeneratedFile,
  SeoAuditProgress,
};

/**
 * ── Example usage in a route ─────────────────────────────────────────────
 *
 * const { SeoAudit, SeoPage, SeoKeyword } = require('./models/seoModels');
 *
 * // Get all audits for a brand
 * const audits = await SeoAudit.findAll({
 *   where: { brand_id: 103 },
 *   order: [['id', 'DESC']],
 *   limit: 10,
 * });
 *
 * // Get all pages for audit #15 with SEO score >= 70
 * const pages = await SeoPage.findAll({
 *   where: { audit_id: 15, seo_grade: ['A+', 'A', 'B'] },
 * });
 *
 * // Get full audit with all related data
 * const full = await SeoAudit.findByPk(15, {
 *   include: [SeoPage, SeoKeyword, SeoBlogTopic, SeoBacklink,
 *             SeoBrokenLink, SeoScorecard, SeoFaq, SeoAxo],
 * });
 */
