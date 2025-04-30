
/// Types generated to aid development. 
/// Generated From: <file name>
pub type Bytes = _rt::Vec<u8>;

pub struct Address {
    pub city: Option<_rt::String>,
    pub country: Option<_rt::String>,
    pub line1: Option<_rt::String>,
    pub line2: Option<_rt::String>,
    pub postal_code: Option<_rt::String>,
    pub state: Option<_rt::String>,
}

pub enum ChargeEventType {
    ChargeCaptured,
    ChargeDisputeClosed,
    ChargeDisputeCreated,
    ChargeDisputeFundsReinstated,
    ChargeDisputeFundsWithdrawn,
    ChargeDisputeUpdated,
    ChargeExpired,
    ChargeFailed,
    ChargePending,
    ChargeRefundUpdated,
    ChargeRefunded,
    ChargeSucceeded,
    ChargeUpdated,
}

pub enum ChargeStatus {
    Failed,
    Pending,
    Succeeded,
}

pub struct Charge {
    pub amount: i32,
    pub amount_captured: i32,
    pub amount_refunded: i32,
    pub balance_transaction: Option<_rt::String>,
    pub calculated_statement_descriptor: Option<_rt::String>,
    pub captured: bool,
    pub created: i32,
    pub currency: _rt::String,
    pub customer: Option<_rt::String>,
    pub description: Option<_rt::String>,
    pub disputed: bool,
    pub event_type: ChargeEventType,
    pub failure_code: Option<_rt::String>,
    pub failure_message: Option<_rt::String>,
    pub id: _rt::String,
    pub invoice: Option<_rt::String>,
    pub paid: bool,
    pub receipt_url: Option<_rt::String>,
    pub refunded: bool,
    pub status: ChargeStatus,
}

pub struct CustomerAddress {
    pub city: Option<_rt::String>,
    pub country: Option<_rt::String>,
    pub line1: Option<_rt::String>,
    pub line2: Option<_rt::String>,
    pub postal_code: Option<_rt::String>,
    pub state: Option<_rt::String>,
}

pub enum CustomerEventType {
    CustomerBankAccountCreated,
    CustomerBankAccountDeleted,
    CustomerBankAccountUpdated,
    CustomerCardCreated,
    CustomerCardDeleted,
    CustomerCardUpdated,
    CustomerCreated,
    CustomerDeleted,
    CustomerSubscriptionCreated,
    CustomerSubscriptionDeleted,
    CustomerSubscriptionPaused,
    CustomerSubscriptionPendingUpdateApplied,
    CustomerSubscriptionPendingUpdateExpired,
    CustomerSubscriptionResumed,
    CustomerSubscriptionTrialWillEnd,
    CustomerSubscriptionUpdated,
    CustomerUpdated,
}

pub struct Customer {
    pub address: Option<CustomerAddress>,
    pub balance: Option<i32>,
    pub created: i32,
    pub currency: Option<_rt::String>,
    pub delinquent: Option<bool>,
    pub description: Option<_rt::String>,
    pub email: Option<_rt::String>,
    pub event_type: CustomerEventType,
    pub id: _rt::String,
    pub invoice_prefix: Option<_rt::String>,
    pub name: Option<_rt::String>,
    pub next_invoice_sequence: Option<i32>,
    pub phone: Option<_rt::String>,
}

pub enum FieldsSectionType {
    Section,
}

pub enum InvoiceBillingReason {
    AutomaticPendingInvoiceItemInvoice,
    Manual,
    QuoteAccept,
    Subscription,
    SubscriptionCreate,
    SubscriptionCycle,
    SubscriptionThreshold,
    SubscriptionUpdate,
    Upcoming,
}

pub enum InvoiceCollectionMethod {
    ChargeAutomatically,
    SendInvoice,
}

pub enum InvoiceEventType {
    InvoiceCreated,
    InvoiceDeleted,
    InvoiceFinalizationFailed,
    InvoiceFinalized,
    InvoiceInvoiceWillBeDue,
    InvoiceMarkedUncollectible,
    InvoiceOverdue,
    InvoicePaid,
    InvoicePaymentActionRequired,
    InvoicePaymentFailed,
    InvoicePaymentSucceeded,
    InvoiceSent,
    InvoiceUpcoming,
    InvoiceUpdated,
    InvoiceVoided,
}

pub enum InvoiceStatus {
    Draft,
    Open,
    Paid,
    Uncollectible,
    Void,
}

pub enum InvoiceitemEventType {
    InvoiceitemCreated,
    InvoiceitemDeleted,
}

#[repr(C)]
pub struct IssuingAuthorizationAmountDetails {
    pub atm_fee: Option<i32>,
    pub cashback_amount: Option<i32>,
}

pub enum IssuingAuthorizationAuthorizationMethod {
    Chip,
    Contactless,
    KeyedIn,
    Online,
    Swipe,
}

pub enum IssuingAuthorizationEventType {
    IssuingAuthorizationCreated,
    IssuingAuthorizationUpdated,
}

pub enum IssuingAuthorizationStatus {
    Closed,
    Pending,
    Reversed,
}

pub enum IssuingCardCancellationReason {
    DesignRejected,
    Lost,
    Stolen,
}

pub struct IssuingCardCardholder {
    pub email: Option<_rt::String>,
    pub id: Option<_rt::String>,
}

pub enum IssuingCardEventType {
    IssuingCardCreated,
    IssuingCardUpdated,
}

pub enum IssuingCardStatus {
    Active,
    Canceled,
    Inactive,
}

pub enum IssuingCardType {
    Physical,
    Virtual,
}

pub struct IssuingCard {
    pub brand: _rt::String,
    pub cancellation_reason: Option<IssuingCardCancellationReason>,
    pub cardholder: IssuingCardCardholder,
    pub created: i32,
    pub currency: _rt::String,
    pub cvc: Option<_rt::String>,
    pub event_type: IssuingCardEventType,
    pub exp_month: i32,
    pub exp_year: i32,
    pub financial_account: Option<_rt::String>,
    pub id: _rt::String,
    pub last4: _rt::String,
    pub status: IssuingCardStatus,
    pub type_: IssuingCardType,
}

#[repr(C)]
pub struct IssuingCardholderDob {
    pub day: Option<i32>,
    pub month: Option<i32>,
    pub year: Option<i32>,
}

pub enum IssuingCardholderEventType {
    IssuingCardholderCreated,
    IssuingCardholderUpdated,
}

pub struct IssuingCardholderIndividual {
    pub dob: Option<IssuingCardholderDob>,
    pub first_name: Option<_rt::String>,
    pub last_name: Option<_rt::String>,
}

pub enum IssuingCardholderStatus {
    Active,
    Blocked,
    Inactive,
}

pub enum IssuingCardholderType {
    Company,
    Individual,
}

pub struct IssuingCardholder {
    pub billing: Address,
    pub created: i32,
    pub email: Option<_rt::String>,
    pub event_type: IssuingCardholderEventType,
    pub id: _rt::String,
    pub individual: Option<IssuingCardholderIndividual>,
    pub name: _rt::String,
    pub phone_number: Option<_rt::String>,
    pub status: IssuingCardholderStatus,
    pub type_: IssuingCardholderType,
}

pub enum IssuingDisputeEventType {
    IssuingDisputeClosed,
    IssuingDisputeCreated,
    IssuingDisputeFundsReinstated,
    IssuingDisputeFundsRescinded,
    IssuingDisputeSubmitted,
    IssuingDisputeUpdated,
}

pub enum IssuingDisputeLossReason {
    CardholderAuthenticationIssuerLiability,
    EcifiveTokenTransactionWithTavv,
    ExcessDisputesInTimeframe,
    HasNotMetTheMinimumDisputeAmountRequirements,
    InvalidDuplicateDispute,
    InvalidIncorrectAmountDispute,
    InvalidNoAuthorization,
    InvalidUseOfDisputes,
    MerchandiseDeliveredOrShipped,
    MerchandiseOrServiceAsDescribed,
    NotCancelled,
    Other,
    RefundIssued,
    SubmittedBeyondAllowableTimeLimit,
    TransactionApprovedAfterPriorFraudDispute,
    TransactionAuthorized,
    TransactionElectronicallyRead,
    TransactionQualifiesForVisaEasyPaymentService,
    TransactionThreeDsRequired,
    TransactionUnattended,
}

pub enum IssuingDisputeReason {
    Canceled,
    Duplicate,
    Fraudulent,
    MerchandiseNotAsDescribed,
    NoValidAuthorization,
    NotReceived,
    Other,
    ServiceNotAsDescribed,
}

pub enum IssuingDisputeStatus {
    Expired,
    Lost,
    Submitted,
    Unsubmitted,
    Won,
}

pub struct IssuingDispute {
    pub amount: i32,
    pub created: i32,
    pub currency: _rt::String,
    pub event_type: IssuingDisputeEventType,
    pub id: _rt::String,
    pub loss_reason: Option<IssuingDisputeLossReason>,
    pub reason: IssuingDisputeReason,
    pub status: IssuingDisputeStatus,
}

pub struct LineItem {
    pub amount: i32,
    pub currency: _rt::String,
    pub description: _rt::String,
}

pub type InvoiceLines = _rt::Vec<LineItem>;

pub struct Invoice {
    pub account_country: Option<_rt::String>,
    pub account_name: Option<_rt::String>,
    pub amount_due: i32,
    pub amount_paid: i32,
    pub amount_remaining: i32,
    pub amount_shipping: i32,
    pub attempt_count: i32,
    pub attempted: bool,
    pub billing_reason: Option<InvoiceBillingReason>,
    pub collection_method: InvoiceCollectionMethod,
    pub created: i32,
    pub currency: _rt::String,
    pub customer: Option<_rt::String>,
    pub customer_email: Option<_rt::String>,
    pub customer_name: Option<_rt::String>,
    pub event_type: InvoiceEventType,
    pub hosted_invoice_url: Option<_rt::String>,
    pub id: Option<_rt::String>,
    pub lines: InvoiceLines,
    pub paid: bool,
    pub paid_out_of_band: bool,
    pub period_end: i32,
    pub period_start: i32,
    pub status: Option<InvoiceStatus>,
    pub subtotal: i32,
    pub total: i32,
}

pub struct MerchantData {
    pub category: _rt::String,
    pub category_code: _rt::String,
    pub city: Option<_rt::String>,
    pub country: Option<_rt::String>,
    pub name: Option<_rt::String>,
    pub network_id: _rt::String,
    pub postal_code: Option<_rt::String>,
    pub state: Option<_rt::String>,
    pub tax_id: Option<_rt::String>,
    pub terminal_id: Option<_rt::String>,
    pub url: Option<_rt::String>,
}

pub struct IssuingAuthorization {
    pub amount: i32,
    pub amount_details: Option<IssuingAuthorizationAmountDetails>,
    pub approved: bool,
    pub authorization_method: IssuingAuthorizationAuthorizationMethod,
    pub card: _rt::String,
    pub cardholder: Option<_rt::String>,
    pub created: i32,
    pub currency: _rt::String,
    pub event_type: IssuingAuthorizationEventType,
    pub id: _rt::String,
    pub merchant_amount: i32,
    pub merchant_currency: _rt::String,
    pub merchant_data: MerchantData,
    pub status: IssuingAuthorizationStatus,
    pub wallet: Option<_rt::String>,
}

pub enum PaymentIntentCancellationReason {
    Abandoned,
    Automatic,
    Duplicate,
    FailedInvoice,
    Fraudulent,
    RequestedByCustomer,
    VoidInvoice,
}

pub enum PaymentIntentCaptureMethod {
    Automatic,
    AutomaticAsync,
    Manual,
}

pub enum PaymentIntentConfirmationMethod {
    Automatic,
    Manual,
}

pub enum PaymentIntentEventType {
    PaymentIntentAmountCapturableUpdated,
    PaymentIntentCanceled,
    PaymentIntentCreated,
    PaymentIntentPartiallyFunded,
    PaymentIntentPaymentFailed,
    PaymentIntentProcessing,
    PaymentIntentRequiresAction,
    PaymentIntentSucceeded,
}

pub type PaymentIntentPaymentMethodTypes = _rt::Vec<_rt::String>;

pub enum PaymentIntentStatus {
    Canceled,
    Processing,
    RequiresAction,
    RequiresCapture,
    RequiresConfirmation,
    RequiresPaymentMethod,
    Succeeded,
}

pub struct PaymentIntent {
    pub amount: i32,
    pub amount_received: Option<i32>,
    pub canceled_at: Option<i32>,
    pub cancellation_reason: Option<PaymentIntentCancellationReason>,
    pub capture_method: PaymentIntentCaptureMethod,
    pub confirmation_method: PaymentIntentConfirmationMethod,
    pub created: i32,
    pub currency: _rt::String,
    pub customer: Option<_rt::String>,
    pub description: Option<_rt::String>,
    pub event_type: PaymentIntentEventType,
    pub id: _rt::String,
    pub invoice: Option<_rt::String>,
    pub payment_method_types: PaymentIntentPaymentMethodTypes,
    pub receipt_email: Option<_rt::String>,
    pub status: PaymentIntentStatus,
}

pub enum PayoutEventType {
    PayoutCanceled,
    PayoutCreated,
    PayoutFailed,
    PayoutPaid,
    PayoutReconciliationCompleted,
    PayoutUpdated,
}

pub enum PayoutReconciliationStatus {
    Completed,
    InProgress,
    NotApplicable,
}

pub enum PayoutType {
    BankAccount,
    Card,
}

pub struct Payout {
    pub amount: i32,
    pub arrival_date: i32,
    pub automatic: bool,
    pub balance_transaction: Option<_rt::String>,
    pub created: i32,
    pub currency: _rt::String,
    pub description: Option<_rt::String>,
    pub event_type: PayoutEventType,
    pub failure_code: Option<_rt::String>,
    pub failure_message: Option<_rt::String>,
    pub id: _rt::String,
    pub method: _rt::String,
    pub reconciliation_status: PayoutReconciliationStatus,
    pub source_type: _rt::String,
    pub statement_descriptor: Option<_rt::String>,
    pub status: _rt::String,
    pub type_: PayoutType,
}

#[repr(C)]
pub struct Period {
    pub end: i32,
    pub start: i32,
}

pub struct Invoiceitem {
    pub amount: i32,
    pub currency: _rt::String,
    pub customer: _rt::String,
    pub date: i32,
    pub description: Option<_rt::String>,
    pub event_type: InvoiceitemEventType,
    pub id: _rt::String,
    pub period: Period,
    pub quantity: i32,
}

pub enum SourceEventType {
    SourceCanceled,
    SourceChargeable,
    SourceFailed,
    SourceMandateNotification,
    SourceRefundAttributesRequired,
    SourceTransactionCreated,
    SourceTransactionUpdated,
}

pub struct SourceOwner {
    pub address: Option<Address>,
    pub email: Option<_rt::String>,
    pub name: Option<_rt::String>,
    pub phone: Option<_rt::String>,
}

pub enum SourceType {
    AchCreditTransfer,
    AchDebit,
    AcssDebit,
    Alipay,
    AuBecsDebit,
    Bancontact,
    Card,
    CardPresent,
    Eps,
    Giropay,
    Ideal,
    Klarna,
    Multibanco,
    PtwentyFour,
    SepaDebit,
    Sofort,
    ThreeDSecure,
    Wechat,
}

pub struct Source {
    pub amount: Option<i32>,
    pub client_secret: _rt::String,
    pub created: i32,
    pub currency: Option<_rt::String>,
    pub customer: Option<_rt::String>,
    pub event_type: SourceEventType,
    pub id: _rt::String,
    pub owner: Option<SourceOwner>,
    pub statement_descriptor: Option<_rt::String>,
    pub status: _rt::String,
    pub type_: SourceType,
}

pub enum SubscriptionDefaultSettingsBillingCycleAnchor {
    Automatic,
    PhaseStart,
}

pub enum SubscriptionDefaultSettingsCollectionMethod {
    ChargeAutomatically,
    SendInvoice,
}

#[repr(C)]
pub struct SubscriptionDefaultSettings {
    pub billing_cycle_anchor: SubscriptionDefaultSettingsBillingCycleAnchor,
    pub collection_method: Option<SubscriptionDefaultSettingsCollectionMethod>,
}

pub enum SubscriptionScheduleEndBehavior {
    Cancel,
    None,
    Release,
    Renew,
}

pub enum SubscriptionScheduleEventType {
    SubscriptionScheduleAborted,
    SubscriptionScheduleCanceled,
    SubscriptionScheduleCompleted,
    SubscriptionScheduleCreated,
    SubscriptionScheduleExpiring,
    SubscriptionScheduleReleased,
    SubscriptionScheduleUpdated,
}

pub enum SubscriptionScheduleStatus {
    Active,
    Canceled,
    Completed,
    NotStarted,
    Released,
}

pub struct SubscriptionSchedule {
    pub canceled_at: Option<i32>,
    pub completed_at: Option<i32>,
    pub created: i32,
    pub customer: _rt::String,
    pub default_settings: SubscriptionDefaultSettings,
    pub end_behavior: SubscriptionScheduleEndBehavior,
    pub event_type: SubscriptionScheduleEventType,
    pub id: _rt::String,
    pub released_at: Option<i32>,
    pub status: SubscriptionScheduleStatus,
}

pub enum TextObjectType {
    Mrkdwn,
}

pub struct TextObject {
    pub text: _rt::String,
    pub type_: TextObjectType,
}

pub type FieldsSectionFields = _rt::Vec<TextObject>;

pub struct FieldsSection {
    pub fields: FieldsSectionFields,
    pub type_: FieldsSectionType,
}

pub enum TextSectionType {
    Section,
}

pub struct TextSection {
    pub text: TextObject,
    pub type_: TextSectionType,
}

pub enum SlackEventUntagged {
    FieldsSection(FieldsSection),
    TextSection(TextSection),
}

pub type SlackEventBlocks = _rt::Vec<SlackEventUntagged>;

pub struct SlackEvent {
    pub blocks: SlackEventBlocks,
}

pub enum TopupEventType {
    TopupCanceled,
    TopupCreated,
    TopupFailed,
    TopupReversed,
    TopupSucceeded,
}

pub enum TopupStatus {
    Canceled,
    Failed,
    Pending,
    Reversed,
    Succeeded,
}

pub struct Topup {
    pub amount: i32,
    pub created: i32,
    pub currency: _rt::String,
    pub description: Option<_rt::String>,
    pub event_type: TopupEventType,
    pub expected_availability_date: Option<i32>,
    pub failure_code: Option<_rt::String>,
    pub failure_message: Option<_rt::String>,
    pub id: _rt::String,
    pub status: TopupStatus,
}

pub enum EventData {
    Charge(Charge),
    Customer(Customer),
    Invoice(Invoice),
    Invoiceitem(Invoiceitem),
    IssuingAuthorization(IssuingAuthorization),
    IssuingCard(IssuingCard),
    IssuingCardholder(IssuingCardholder),
    IssuingDispute(IssuingDispute),
    PaymentIntent(PaymentIntent),
    Payout(Payout),
    Source(Source),
    SubscriptionSchedule(SubscriptionSchedule),
    Topup(Topup),
}

pub struct StripeEvent {
    pub api_version: Option<_rt::String>,
    pub created: i32,
    pub data: EventData,
    pub fluvio_version: _rt::String,
    pub id: _rt::String,
    pub livemode: bool,
    pub pending_webhooks: i32,
}